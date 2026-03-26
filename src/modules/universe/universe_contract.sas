/* -------------------------------------------------------------------------
universe_contract.sas - Validate the unified scope table consumed by
Universe and confirm that TRAIN and OOT windows have coverage.
------------------------------------------------------------------------- */
%macro universe_contract(input_caslib=, input_table=, byvar=, id_var=,
    monto_var=, train_min_mes=, train_max_mes=, oot_min_mes=, oot_max_mes=);

    %let _univ_rc=0;

    %local _univ_table_exists _univ_has_col _univ_nobs_scope _univ_nobs_trn
        _univ_nobs_oot;

    %if %length(%superq(input_table))=0 %then %do;
        %put ERROR: [universe_contract] input_table no definida.;
        %let _univ_rc=1;
        %return;
    %end;

    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [universe_contract] Variable temporal (byvar) no definida.;
        %let _univ_rc=1;
        %return;
    %end;

    %if %length(%superq(id_var))=0 %then %do;
        %put ERROR: [universe_contract] Variable ID (id_var) no definida.;
        %let _univ_rc=1;
        %return;
    %end;

    %if %length(%superq(train_min_mes))=0 or %length(%superq(train_max_mes))=0
        or %length(%superq(oot_min_mes))=0 or
        %length(%superq(oot_max_mes))=0 %then %do;
        %put ERROR: [universe_contract] Ventanas TRAIN/OOT no definidas.;
        %let _univ_rc=1;
        %return;
    %end;

    %let _univ_table_exists=0;
    proc sql noprint;
        select count(*) into :_univ_table_exists trimmed
        from dictionary.tables
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.");
    quit;

    %if &_univ_table_exists.=0 %then %do;
        %put ERROR: [universe_contract] &input_caslib..&input_table. no existe.;
        %let _univ_rc=1;
        %return;
    %end;

    %let _univ_has_col=0;
    proc sql noprint;
        select count(*) into :_univ_has_col trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.")
          and upcase(name)=upcase("&byvar.");
    quit;
    %if &_univ_has_col.=0 %then %do;
        %put ERROR: [universe_contract] byvar=&byvar. no encontrada en &input_table..;
        %let _univ_rc=1;
        %return;
    %end;

    %let _univ_has_col=0;
    proc sql noprint;
        select count(*) into :_univ_has_col trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.")
          and upcase(name)=upcase("&id_var.");
    quit;
    %if &_univ_has_col.=0 %then %do;
        %put ERROR: [universe_contract] id_var=&id_var. no encontrada en &input_table..;
        %let _univ_rc=1;
        %return;
    %end;

    proc fedsql sessref=conn;
        create table casuser._univ_contract_counts {options replace=true} as
        select count(*) as N_Scope,
               sum(case
                       when &byvar. >= &train_min_mes.
                        and &byvar. <= &train_max_mes.
                       then 1
                       else 0
                   end) as N_Train,
               sum(case
                       when &byvar. >= &oot_min_mes.
                        and &byvar. <= &oot_max_mes.
                       then 1
                       else 0
                   end) as N_OOT
        from &input_caslib..&input_table.;
    quit;

    data _null_;
        set casuser._univ_contract_counts;
        call symputx('_univ_nobs_scope', N_Scope);
        call symputx('_univ_nobs_trn', N_Train);
        call symputx('_univ_nobs_oot', N_OOT);
    run;

    proc datasets library=casuser nolist nowarn;
        delete _univ_contract_counts;
    quit;

    %if %sysevalf(%superq(_univ_nobs_scope)=, boolean) %then
        %let _univ_nobs_scope=0;
    %if %sysevalf(%superq(_univ_nobs_trn)=, boolean) %then
        %let _univ_nobs_trn=0;
    %if %sysevalf(%superq(_univ_nobs_oot)=, boolean) %then
        %let _univ_nobs_oot=0;

    %if &_univ_nobs_scope.=0 %then %do;
        %put ERROR: [universe_contract] &input_caslib..&input_table. tiene 0 obs.;
        %let _univ_rc=1;
        %return;
    %end;

    %if &_univ_nobs_trn.=0 %then %do;
        %put ERROR: [universe_contract] La ventana TRAIN no tiene observaciones en &input_caslib..&input_table..;
        %let _univ_rc=1;
        %return;
    %end;

    %if &_univ_nobs_oot.=0 %then %do;
        %put ERROR: [universe_contract] La ventana OOT no tiene observaciones en &input_caslib..&input_table..;
        %let _univ_rc=1;
        %return;
    %end;

    %if %length(%superq(monto_var)) > 0 %then %do;
        %let _univ_has_col=0;
        proc sql noprint;
            select count(*) into :_univ_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.")
              and upcase(name)=upcase("&monto_var.");
        quit;
        %if &_univ_has_col.=0 %then %put WARNING: [universe_contract]
            monto_var=&monto_var. no encontrada. Se omitiran analisis de monto.;
    %end;
    %else %do;
        %put WARNING: [universe_contract] monto_var no definida. Se omitiran
            analisis de monto.;
    %end;

    %put NOTE: [universe_contract] OK - base=&_univ_nobs_scope. obs,
        TRAIN=&_univ_nobs_trn. obs, OOT=&_univ_nobs_oot. obs;
    %put NOTE: [universe_contract] byvar=&byvar. id_var=&id_var.
        monto_var=&monto_var.;

%mend universe_contract;
