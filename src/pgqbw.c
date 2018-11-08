#include "postgres.h"
#include "fmgr.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
//#include "storage/latch.h"
//#include "storage/lwlock.h"
//#include "storage/proc.h"
//#include "storage/shmem.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "utils/snapmgr.h"
#include "pgstat.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

void _PG_init(void);
//void _PG_fini(void);
void pgqbw_main(Datum arg);

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr1 = false;
static char *initial_database = NULL;
static char *initial_username = NULL;
static int check_period = 60;
//static emit_log_hook_type prev_log_hook = NULL;

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

static void pgqbw_sigusr1(SIGNAL_ARGS) {
    int save_errno = errno;
    got_sigusr1 = true;
    SetLatch(MyLatch);
    errno = save_errno;
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

void pgqbw_main(Datum main_arg) {
    StringInfoData buf;
    pqsignal(SIGHUP, pgqbw_sighup);
    pqsignal(SIGTERM, pgqbw_sigterm);
    pqsignal(SIGUSR1, pgqbw_sigusr1);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection(initial_database, initial_username, 0);
    initialize_pgqbw();
    initStringInfo(&buf);
    appendStringInfo(&buf, "WITH s AS ("
    "    SELECT  datname,"
    "            (SELECT * FROM dblink.dblink('dbname='||datname, 'SELECT nspname FROM pg_namespace WHERE nspname = ''pgq''') AS (text TEXT)) AS nspname"
    "    FROM    pg_database"
    "    WHERE   NOT datistemplate"
    "    AND     datallowconn"
    ") SELECT datname FROM s WHERE nspname IS NOT NULL");
//    EnableNotifyInterrupt();
//    pgstat_report_activity(STATE_RUNNING, "background_worker");
//    StartTransactionCommand();
//    Async_Listen("foo");
//    CommitTransactionCommand();
//    pgstat_report_activity(STATE_IDLE, NULL);
    while (!got_sigterm) {
        int ret;
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, check_period * 1000L, PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
        if (rc & WL_POSTMASTER_DEATH) proc_exit(1);
        CHECK_FOR_INTERRUPTS();
        if (got_sighup) {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        /*if (got_sigusr1) {
            got_sigusr1 = false;
            elog(INFO, " background_worker: notification received");
            // DO SOME WORK WITH STORED NOTIFICATIONS
        }*/
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());
        pgstat_report_activity(STATE_RUNNING, buf.data);
        ret = SPI_execute(buf.data, false, 0);
        if (ret != SPI_OK_SELECT) elog(FATAL, "ret != SPI_OK_SELECT: buf.data=%s, ret=%d", buf.data, ret);
        for (unsigned i = 0; i < SPI_processed; i++) {
            bool isnull;
            char *datname = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
            elog(LOG, "datname=%s", datname);
        }
        SPI_finish();
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_stat(false);
        pgstat_report_activity(STATE_IDLE, NULL);
    }
//    elog(LOG, "background_worker: finished");
    proc_exit(1);
}

/*static void store_notification(ErrorData *edata) {
    // HERE STORE THE NOTIFICATION FROM SERVER LOG
    if (prev_log_hook) (*prev_log_hook) (edata);
}*/

void _PG_init(void) {
    BackgroundWorker worker;
    if (!process_shared_preload_libraries_in_progress) return;
    DefineCustomStringVariable("pgqbw.initial_database", "startup database to query other databases", NULL, &initial_database, "postgres", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomStringVariable("pgqbw.initial_username", "startup username to query other databases", NULL, &initial_username, "postgres", PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pgqbw.check_period", "how often to check for new databases", NULL, &check_period, 60, 1, INT_MAX, PGC_SIGHUP, 0, NULL, NULL, NULL);
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "pgqbw");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "pgqbw_main");
    snprintf(worker.bgw_name, BGW_MAXLEN, "queue background worker");
    snprintf(worker.bgw_type, BGW_MAXLEN, "pgqbw");
    worker.bgw_notify_pid = 0;
//    worker.bgw_main = pgqbw_main;
    worker.bgw_main_arg = (Datum) 0;
    RegisterBackgroundWorker(&worker);
//    prev_log_hook = emit_log_hook;
//    emit_log_hook = store_notification;
}

//void _PG_fini(void) {
    //emit_log_hook = prev_log_hook;
//}
