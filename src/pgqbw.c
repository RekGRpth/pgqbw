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
void pgqbw_main(Datum arg);
void pgqbw_ticker(Datum arg);

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static char *initial_database = NULL;
static char *initial_username = NULL;
static int check_period = 60;

static void pgqbw_sighup(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void pgqbw_sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

void pgqbw_ticker(Datum arg) {
    char *datname = MyBgworkerEntry->bgw_extra;
    char *usename = datname + strlen(datname) + 1;
    elog(LOG, "datname=%s, usename=%s", datname, usename);
    pqsignal(SIGHUP, pgqbw_sighup);
    pqsignal(SIGTERM, pgqbw_sigterm);
    BackgroundWorkerUnblockSignals();
    elog(LOG, "pgqbw_ticker finished");
    proc_exit(0);
}

static void initialize_pgqbw() {
    int ret, ntup;
    bool isnull;
    StringInfoData buf;
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT COUNT(*) FROM pg_namespace WHERE nspname = 'dblink'");
    pgstat_report_activity(STATE_RUNNING, buf.data);
    ret = SPI_execute(buf.data, true, 0);
    if (ret != SPI_OK_SELECT) elog(FATAL, "ret != SPI_OK_SELECT: buf.data=%s, ret=%d", buf.data, ret);
    if (SPI_processed != 1) elog(FATAL, "SPI_processed != 1");
    ntup = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    if (isnull) elog(FATAL, "isnull");
    if (ntup == 0) {
        resetStringInfo(&buf);
        appendStringInfo(&buf, "CREATE SCHEMA IF NOT EXISTS dblink; CREATE EXTENSION IF NOT EXISTS dblink SCHEMA dblink;");
        pgstat_report_activity(STATE_RUNNING, buf.data);
        SetCurrentStatementStartTimestamp();
        ret = SPI_execute(buf.data, false, 0);
        if (ret != SPI_OK_UTILITY) elog(FATAL, "ret != SPI_OK_UTILITY: buf.data=%s, ret=%d", buf.data, ret);
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
    snprintf(worker.bgw_function_name, sizeof("pgqbw_ticker"), "pgqbw_ticker");
    snprintf(worker.bgw_name, BGW_MAXLEN, "%s %s ticker background worker", datname, usename);
    snprintf(worker.bgw_type, sizeof("ticker background worker"), "ticker background worker");
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

void pgqbw_main(Datum main_arg) {
    StringInfoData buf;
    pqsignal(SIGHUP, pgqbw_sighup);
    pqsignal(SIGTERM, pgqbw_sigterm);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(initial_database, initial_username, 0);
    initialize_pgqbw();
    initStringInfo(&buf);
    appendStringInfo(&buf, "WITH subquery AS ( "
        "SELECT datname, usename, "
        "(SELECT nspname FROM dblink.dblink('dbname='||datname, 'SELECT nspname FROM pg_namespace WHERE nspname = ''pgq''') AS (nspname TEXT)) AS nspname "
        "FROM pg_database "
        "INNER JOIN pg_user ON usesysid = datdba "
        "WHERE NOT datistemplate "
        "AND datallowconn "
    ") SELECT datname, usename FROM subquery WHERE nspname IS NOT NULL");
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
        pgstat_report_activity(STATE_RUNNING, buf.data);
        ret = SPI_execute(buf.data, false, 0);
        if (ret != SPI_OK_SELECT) elog(FATAL, "ret != SPI_OK_SELECT: buf.data=%s, ret=%d", buf.data, ret);
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
    elog(LOG, "pgqbw_main finished");
    proc_exit(0);
}

void _PG_init(void) {
    BackgroundWorker worker;
    if (IsBinaryUpgrade) return;
    if (!process_shared_preload_libraries_in_progress) ereport(ERROR, (errmsg("pgqbw can only be loaded via shared_preload_libraries"), errhint("Add pgqbw to the shared_preload_libraries configuration variable in postgresql.conf.")));
    DefineCustomStringVariable("pgqbw.initial_database", "startup database to query other databases", NULL, &initial_database, "postgres", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pgqbw.initial_username", "startup username to query other databases", NULL, &initial_username, "postgres", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pgqbw.check_period", "how often to check for new databases", NULL, &check_period, 60, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;
    snprintf(worker.bgw_library_name, sizeof("pgqbw"), "pgqbw");
    snprintf(worker.bgw_function_name, sizeof("pgqbw_main"), "pgqbw_main");
    snprintf(worker.bgw_name, sizeof("queue background worker launcher"), "queue background worker launcher");
    snprintf(worker.bgw_type, sizeof("queue background worker launcher"), "queue background worker launcher");
    worker.bgw_notify_pid = 0;
    worker.bgw_main_arg = (Datum) 0;
    RegisterBackgroundWorker(&worker);
}
