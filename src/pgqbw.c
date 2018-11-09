#include "postgres.h"
#include "fmgr.h"

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "utils/snapmgr.h"
#include "pgstat.h"
#include "utils/guc.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void launcher(Datum arg);
void ticker(Datum arg);

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static char *initial_database = NULL;
static char *initial_username = NULL;
static int stats_period = 30;
static int check_period = 60;
static int retry_period = 30;
static int maint_period = 120;
static int ticker_period = 1;
static unsigned long int time_time = 0;
static unsigned long int next_ticker = 0;
static unsigned long int next_maint = 0;
static unsigned long int next_retry = 0;
static unsigned long int next_stats = 0;
static unsigned long int n_ticks = 0;
static unsigned long int n_maint = 0;
static unsigned long int n_retry = 0;

static void sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static inline int min(int a, int b, int c) {
    int m = a;
    if (m > b) m = b;
    if (m > c) return c;
    return m;
}

static void initialize_ticker() {
    int ret;
    bool isnull, lock;
    char *sql = "SELECT pg_try_advisory_lock(pg_database.oid::INT, pg_namespace.oid::INT) FROM pg_database, pg_namespace WHERE datname = current_catalog AND nspname = 'pgq'";
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    pgstat_report_activity(STATE_RUNNING, sql);
    ret = SPI_execute(sql, true, 0);
    if (ret != SPI_OK_SELECT) elog(FATAL, "ret != SPI_OK_SELECT: sql=%s, ret=%d", sql, ret);
    if (SPI_processed != 1) elog(FATAL, "SPI_processed != 1");
    lock = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    if (isnull) elog(FATAL, "isnull");
    if (!lock) elog(FATAL, "already running");
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    pgstat_report_activity(STATE_IDLE, NULL);
}

void ticker(Datum arg) {
    StringInfoData buf;
    char *sql;
    char *datname = MyBgworkerEntry->bgw_extra;
    char *usename = datname + strlen(datname) + 1;
    elog(LOG, "ticker started datname=%s, usename=%s", datname, usename);
    pqsignal(SIGHUP, sighup);
    pqsignal(SIGTERM, sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(datname, usename, 0);
    initialize_ticker();
    initStringInfo(&buf);
    while (!got_sigterm) {
        int ret;
        int period = min(retry_period, maint_period, ticker_period);
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, period * 1000L, PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
        if (rc & WL_POSTMASTER_DEATH) proc_exit(1);
        if (rc & WL_TIMEOUT) time_time += period;
        if (got_sighup) {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        if (time_time >= next_ticker) {
            SetCurrentStatementStartTimestamp();
            StartTransactionCommand();
            SPI_connect();
            PushActiveSnapshot(GetTransactionSnapshot());
            sql = "SELECT pgq.ticker()";
            pgstat_report_activity(STATE_RUNNING, sql);
            ret = SPI_execute(sql, false, 0);
            if (ret != SPI_OK_SELECT) elog(FATAL, "ret != SPI_OK_SELECT: sql=%s, ret=%d", sql, ret);
            if (SPI_processed == 1) n_ticks++;
            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();
            pgstat_report_stat(false);
            pgstat_report_activity(STATE_IDLE, NULL);
            next_ticker = time_time + ticker_period;
        }
        if (time_time >= next_maint) {
            SetCurrentStatementStartTimestamp();
            StartTransactionCommand();
            SPI_connect();
            PushActiveSnapshot(GetTransactionSnapshot());
            sql = "SELECT func_name, func_arg FROM pgq.maint_operations()";
            pgstat_report_activity(STATE_RUNNING, sql);
            ret = SPI_execute(sql, false, 0);
            if (ret != SPI_OK_SELECT) elog(FATAL, "ret != SPI_OK_SELECT: sql=%s, ret=%d", sql, ret);
            resetStringInfo(&buf);
            for (unsigned i = 0; i < SPI_processed; i++) {
                bool isnull;
                char *func_name = NULL, *func_arg = NULL;
                func_name = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
                func_arg = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull));
                elog(LOG, "datname=%s, usename=%s, func_name=%s, func_arg=%s", datname, usename, func_name, func_arg);
                if (!strncasecmp(func_name, "vacuum", sizeof("vacuum") - 1)) {
                    appendStringInfo(&buf, "%s \"%s\";", func_name, func_arg);
                } else if (isnull) {
                    appendStringInfo(&buf, "SELECT %s();", func_name);
                } else {
                    appendStringInfo(&buf, "SELECT %s('%s');", func_name, func_arg);
                }
                n_maint++;
            }
            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();
            pgstat_report_stat(false);
            pgstat_report_activity(STATE_IDLE, NULL);
            next_maint = time_time + maint_period;
        }
        if (time_time >= next_retry) {
            SetCurrentStatementStartTimestamp();
            StartTransactionCommand();
            SPI_connect();
            PushActiveSnapshot(GetTransactionSnapshot());
            sql = "SELECT * FROM pgq.maint_retry_events();";
            pgstat_report_activity(STATE_RUNNING, sql);
            for (int retry = 1; retry; ) {
                int ret = SPI_execute(sql, false, 0);
                if (ret != SPI_OK_SELECT) elog(FATAL, "ret != SPI_OK_SELECT: sql=%s, ret=%d", sql, ret);
                if (SPI_processed == 1) {
                    bool isnull;
                    retry = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
                    n_retry += retry;
                } else retry = 0;
            }
            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();
            pgstat_report_stat(false);
            pgstat_report_activity(STATE_IDLE, NULL);
            next_retry = time_time + retry_period;
        }
        if (time_time >= next_stats) {
            elog(LOG, "datname=%s, usename=%s, time_time=%lu, n_ticks=%lu, n_maint=%lu, n_retry=%lu", datname, usename, time_time, n_ticks, n_maint, n_retry);
            next_stats = time_time + stats_period;
        }
    }
    elog(LOG, "ticker finished datname=%s, usename=%s", datname, usename);
    proc_exit(0);
}

static void initialize_launcher() {
    int ret, ntup;
    bool isnull;
    char *sql = "SELECT COUNT(*) FROM pg_namespace WHERE nspname = 'dblink'";
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    pgstat_report_activity(STATE_RUNNING, sql);
    ret = SPI_execute(sql, true, 0);
    if (ret != SPI_OK_SELECT) elog(FATAL, "ret != SPI_OK_SELECT: sql=%s, ret=%d", sql, ret);
    if (SPI_processed != 1) elog(FATAL, "SPI_processed != 1");
    ntup = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    if (isnull) elog(FATAL, "isnull");
    if (ntup == 0) {
        sql = "CREATE SCHEMA IF NOT EXISTS dblink; CREATE EXTENSION IF NOT EXISTS dblink SCHEMA dblink";
        pgstat_report_activity(STATE_RUNNING, sql);
        SetCurrentStatementStartTimestamp();
        ret = SPI_execute(sql, false, 0);
        if (ret != SPI_OK_UTILITY) elog(FATAL, "ret != SPI_OK_UTILITY: sql=%s, ret=%d", sql, ret);
    }
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    pgstat_report_activity(STATE_IDLE, NULL);
}

static void launch_ticker(char *datname, char *usename) {
    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;
    pid_t pid;
    MemoryContext oldcontext;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;
    snprintf(worker.bgw_library_name, sizeof("pgqbw"), "pgqbw");
    snprintf(worker.bgw_function_name, sizeof("ticker"), "ticker");
    snprintf(worker.bgw_name, BGW_MAXLEN, "%s %s pgqbw worker", datname, usename);
    snprintf(worker.bgw_type, sizeof("pgqbw worker"), "pgqbw worker");
    snprintf(worker.bgw_extra + snprintf(worker.bgw_extra, strlen(datname) + 1, "%s", datname) + 1, strlen(usename) + 1, "%s", usename);
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_main_arg = (Datum) 0;
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not register background process"), errhint("You may need to increase max_worker_processes.")));
    MemoryContextSwitchTo(oldcontext);
    switch (WaitForBackgroundWorkerStartup(handle, &pid)) {
        case BGWH_STARTED: break;
        case BGWH_STOPPED:
            pfree(handle);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("could not start background process"), errhint("More details may be available in the server log.")));
            break;
        case BGWH_POSTMASTER_DIED:
            pfree(handle);
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("cannot start background processes without postmaster"), errhint("Kill all remaining database processes and restart the database.")));
            break;
        default:
            elog(ERROR, "unexpected bgworker handle status");
            break;
    }
}

void launcher(Datum main_arg) {
    char *sql = "WITH subquery AS ( "
        "SELECT datname, usename, "
        "(SELECT lock FROM dblink.dblink('dbname='||datname||' user='||usename, 'SELECT pg_try_advisory_lock(pg_database.oid::INT, pg_namespace.oid::INT) FROM pg_database, pg_namespace WHERE datname = current_catalog AND nspname = ''pgq'';') AS (lock bool)) AS lock "
        "FROM pg_database "
        "INNER JOIN pg_user ON usesysid = datdba "
        "WHERE NOT datistemplate "
        "AND datallowconn "
    ") SELECT datname, usename FROM subquery WHERE lock IS NOT NULL and lock";
    elog(LOG, "launcher started initial_database=%s, initial_username=%s", initial_database, initial_username);
    pqsignal(SIGHUP, sighup);
    pqsignal(SIGTERM, sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(initial_database, initial_username, 0);
    initialize_launcher();
    while (!got_sigterm) {
        int ret;
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, check_period * 1000L, PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
        if (rc & WL_POSTMASTER_DEATH) proc_exit(1);
        if (got_sighup) {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());
        pgstat_report_activity(STATE_RUNNING, sql);
        ret = SPI_execute(sql, false, 0);
        if (ret != SPI_OK_SELECT) elog(FATAL, "ret != SPI_OK_SELECT: sql=%s, ret=%d", sql, ret);
        for (unsigned i = 0; i < SPI_processed; i++) {
            bool isnull;
            char *datname = NULL, *usename = NULL;
            datname = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
            usename = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull));
            elog(LOG, "datname=%s, usename=%s", datname, usename);
            launch_ticker(datname, usename);
        }
        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_stat(false);
        pgstat_report_activity(STATE_IDLE, NULL);
    }
    elog(LOG, "launcher finished initial_database=%s, initial_username=%s", initial_database, initial_username);
    proc_exit(0);
}

void _PG_init(void) {
    BackgroundWorker worker;
    if (IsBinaryUpgrade) return;
    if (!process_shared_preload_libraries_in_progress) ereport(ERROR, (errmsg("pgqbw can only be loaded via shared_preload_libraries"), errhint("Add pgqbw to the shared_preload_libraries configuration variable in postgresql.conf.")));
    DefineCustomStringVariable("pgqbw.initial_database", "startup database to query other databases", NULL, &initial_database, "postgres", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pgqbw.initial_username", "startup username to query other databases", NULL, &initial_username, "postgres", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pgqbw.check_period", "how often to check for new databases", NULL, &check_period, 60, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pgqbw.retry_period", "how often to flush retry queue", NULL, &retry_period, 30, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pgqbw.maint_period", "how often to do maintentance", NULL, &maint_period, 120, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pgqbw.ticker_period", "how often to run ticker", NULL, &ticker_period, 1, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pgqbw.stats_period", "how often to print statistics", NULL, &stats_period, 30, min(retry_period, maint_period, ticker_period), INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;
    snprintf(worker.bgw_library_name, sizeof("pgqbw"), "pgqbw");
    snprintf(worker.bgw_function_name, sizeof("launcher"), "launcher");
    snprintf(worker.bgw_name, BGW_MAXLEN, "%s %s pgqbw launcher", initial_database, initial_username);
    snprintf(worker.bgw_type, sizeof("pgqbw launcher"), "pgqbw launcher");
    worker.bgw_notify_pid = 0;
    worker.bgw_main_arg = (Datum) 0;
    RegisterBackgroundWorker(&worker);
}
