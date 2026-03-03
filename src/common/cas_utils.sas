/* =========================================================================
   cas_utils.sas — Baseline CAS utility macros
   Ref: docs/caslib_lifecycle.md

   Provides:
     %_create_caslib  — Create a PATH-based CASLIB (GLOBAL by default)
     %_drop_caslib    — Drop CASLIB and optionally its tables
     %_load_cas_data  — Load .sashdat from a CASLIB into CAS
     %_save_into_caslib — Save a CAS table as .sashdat into a CASLIB
     %_promote_castable — Load + promote a table (temporary; must cleanup)

   Convention: casuser is reserved for config tables only.
   All data/output persistence uses PATH-based CASLIBs.
   ========================================================================= */

/* ------------------------------------------------------------------
   %_create_caslib — Create a PATH-based CASLIB
   Parameters:
     cas_path        = filesystem path for the CASLIB
     caslib_name     = logical CASLIB name
     lib_caslib      = SAS libref to assign
     global          = Y (default) for GLOBAL scope
     cas_sess_name   = CAS session name (default: conn)
     term_global_sess= 1 to terminate session after creation (default: 1)
     subdirs_flg     = 1 to enable subdirectory access (default: 0)
   ------------------------------------------------------------------ */
%macro _create_caslib(
    cas_path =,
    caslib_name =,
    lib_caslib =,
    global = Y,
    cas_sess_name = conn,
    term_global_sess=1,
    subdirs_flg=0
);
    %if %sysfunc(sessfound(&cas_sess_name)) = 0 %then %do;
        cas &cas_sess_name.;
    %end;

    proc cas;
        session &cas_sess_name.;
        table.dropcaslib / caslib="&caslib_name." quiet=true;
    quit;

    %if &global. = Y %then %do;
        caslib &caslib_name. datasource=(srctype="path") path="&cas_path."
            sessref=&cas_sess_name. global libref=&lib_caslib.
            %if &subdirs_flg. = 1 %then %do; subdirs %end;
        ;
        %if &term_global_sess. = 1 %then %do;
            cas &cas_sess_name. terminate;
        %end;
    %end;
    %else %do;
        caslib &caslib_name. datasource=(srctype="path") path="&cas_path."
            sessref=&cas_sess_name. libref=&lib_caslib.;
    %end;
%mend _create_caslib;

/* ------------------------------------------------------------------
   %_drop_caslib — Drop a CASLIB; optionally drop all tables first
   ------------------------------------------------------------------ */
%macro _drop_caslib(
    caslib_name =,
    cas_sess_name=,
    del_prom_tables=0
);
    %if %sysfunc(sessfound(&cas_sess_name)) = 0 %then %do;
        cas &cas_sess_name.;
    %end;

    %if &del_prom_tables = 1 %then %do;
        proc cas;
            table.tableInfo result=r / caslib="&caslib_name.";
            if r.TableInfo.nrows > 0 then do;
                do i = 1 to r.TableInfo.nrows;
                    table.dropTable / caslib="&caslib_name."
                        name=r.TableInfo[i, "Name"] quiet=true;
                end;
            end;
        quit;
    %end;

    proc cas;
        session &cas_sess_name.;
        table.dropcaslib / caslib="&caslib_name." quiet=true;
    quit;
%mend _drop_caslib;

/* ------------------------------------------------------------------
   %_load_cas_data — Load a .sashdat file from a CASLIB into CAS
   ------------------------------------------------------------------ */
%macro _load_cas_data(
    caslib_name=,
    cas_sess_name=,
    output_data_name=
);
    %if %sysfunc(sessfound(&cas_sess_name)) = 0 %then %do;
        cas &cas_sess_name.;
    %end;

    proc casutil;
        load casdata="&output_data_name..sashdat"
            incaslib="&caslib_name"
            casout="&output_data_name"
            outcaslib="&caslib_name";
    quit;
%mend _load_cas_data;

/* ------------------------------------------------------------------
   %_save_into_caslib — Save a CAS table as .sashdat into a CASLIB
   ------------------------------------------------------------------ */
%macro _save_into_caslib(
    m_cas_sess_name=,
    m_input_caslib=,
    m_input_data=,
    m_output_caslib=,
    m_subdir_data=
);
    %if %sysfunc(sessfound(&m_cas_sess_name)) = 0 %then %do;
        cas &m_cas_sess_name.;
    %end;

    proc casutil;
        save incaslib="&m_input_caslib" casdata="&m_input_data"
            outcaslib="&m_output_caslib" casout="&m_subdir_data..sashdat"
            replace;
    quit;
%mend _save_into_caslib;

/* ------------------------------------------------------------------
   %_promote_castable — Load .sashdat and promote into a global CASLIB
   IMPORTANT: caller MUST drop promoted table at end of step (Rule 2).
   ------------------------------------------------------------------ */
%macro _promote_castable(
    m_cas_sess_name=,
    m_input_caslib=, m_subdir_data=,
    m_output_caslib=, m_output_data=
);
    %if %sysfunc(sessfound(&m_cas_sess_name)) = 0 %then %do;
        cas &m_cas_sess_name.;
    %end;

    proc casutil;
        load incaslib="&m_input_caslib" casdata="&m_subdir_data..sashdat"
            outcaslib="&m_output_caslib" casout="&m_output_data" replace;
    quit;

    proc casutil;
        promote incaslib="&m_output_caslib" casdata="&m_output_data"
            outcaslib="&m_output_caslib" casout="&m_output_data";
    quit;
%mend _promote_castable;

%put NOTE: [cas_utils] CAS utility macros loaded.;
