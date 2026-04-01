/* =========================================================================
monotonicidad_contract.sas - Validaciones pre-ejecucion del modulo
Monotonicidad (METOD7)

Valida el flujo scope-input:
- input_table accesible
- score_var (PD), target y byvar definidos y presentes
- def_cld y ventanas TRAIN/OOT definidos
- cobertura efectiva TRAIN y OOT sobre la tabla unificada

Setea macro variable &_mono_rc:
0 = OK, 1 = fallo
========================================================================= */
%macro monotonicidad_contract(input_caslib=, input_table=, score_var=, target=,
    byvar=, def_cld=, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=);

    %global _mono_rc;
    %let _mono_rc=0;

    %local _mono_table_exists _mono_has_col _mono_nobs_scope _mono_nobs_trn
        _mono_nobs_oot;

    %if %length(%superq(input_table))=0 %then %do;
        %put ERROR: [monotonicidad_contract] input_table no definida.;
        %let _mono_rc=1;
        %return;
    %end;

    %if %length(%superq(score_var))=0 %then %do;
        %put ERROR: [monotonicidad_contract] score_var (pd) no definida.;
        %let _mono_rc=1;
        %return;
    %end;

    %if %length(%superq(target))=0 %then %do;
        %put ERROR: [monotonicidad_contract] Variable target no definida.;
        %let _mono_rc=1;
        %return;
    %end;

    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [monotonicidad_contract] Variable temporal (byvar)
            no definida.;
        %let _mono_rc=1;
        %return;
    %end;

    %if %length(%superq(def_cld))=0 or %length(%superq(train_min_mes))=0 or
        %length(%superq(train_max_mes))=0 or %length(%superq(oot_min_mes))=0
        or %length(%superq(oot_max_mes))=0 %then %do;
        %put ERROR: [monotonicidad_contract] Ventanas TRAIN/OOT o def_cld no
            definidos.;
        %let _mono_rc=1;
        %return;
    %end;

    %let _mono_table_exists=0;
    proc sql noprint;
        select count(*) into :_mono_table_exists trimmed
        from dictionary.tables
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.");
    quit;

    %if &_mono_table_exists.=0 %then %do;
        %put ERROR: [monotonicidad_contract] &input_caslib..&input_table.
            no existe.;
        %let _mono_rc=1;
        %return;
    %end;

    %let _mono_has_col=0;
    proc sql noprint;
        select count(*) into :_mono_has_col trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.")
          and upcase(name)=upcase("&score_var.");
    quit;
    %if &_mono_has_col.=0 %then %do;
        %put ERROR: [monotonicidad_contract] score_var=&score_var. no
            encontrada en &input_caslib..&input_table..;
        %let _mono_rc=1;
        %return;
    %end;

    %let _mono_has_col=0;
    proc sql noprint;
        select count(*) into :_mono_has_col trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.")
          and upcase(name)=upcase("&target.");
    quit;
    %if &_mono_has_col.=0 %then %do;
        %put ERROR: [monotonicidad_contract] target=&target. no encontrada
            en &input_caslib..&input_table..;
        %let _mono_rc=1;
        %return;
    %end;

    %let _mono_has_col=0;
    proc sql noprint;
        select count(*) into :_mono_has_col trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.")
          and upcase(name)=upcase("&byvar.");
    quit;
    %if &_mono_has_col.=0 %then %do;
        %put ERROR: [monotonicidad_contract] byvar=&byvar. no encontrada
            en &input_caslib..&input_table..;
        %let _mono_rc=1;
        %return;
    %end;

    proc fedsql sessref=conn;
        create table casuser._mono_contract_counts {options replace=true} as
        select count(*) as N_Scope,
               sum(case
                       when &byvar. <= &def_cld.
                        and &byvar. >= &train_min_mes.
                        and &byvar. <= &train_max_mes.
                       then 1
                       else 0
                   end) as N_Train,
               sum(case
                       when &byvar. <= &def_cld.
                        and &byvar. >= &oot_min_mes.
                        and &byvar. <= &oot_max_mes.
                       then 1
                       else 0
                   end) as N_OOT
        from &input_caslib..&input_table.;
    quit;

    data _null_;
        set casuser._mono_contract_counts;
        call symputx('_mono_nobs_scope', N_Scope);
        call symputx('_mono_nobs_trn', N_Train);
        call symputx('_mono_nobs_oot', N_OOT);
    run;

    proc datasets library=casuser nolist nowarn;
        delete _mono_contract_counts;
    quit;

    %if %sysevalf(%superq(_mono_nobs_scope)=, boolean) %then
        %let _mono_nobs_scope=0;
    %if %sysevalf(%superq(_mono_nobs_trn)=, boolean) %then
        %let _mono_nobs_trn=0;
    %if %sysevalf(%superq(_mono_nobs_oot)=, boolean) %then
        %let _mono_nobs_oot=0;

    %if &_mono_nobs_scope.=0 %then %do;
        %put ERROR: [monotonicidad_contract] &input_caslib..&input_table.
            tiene 0 obs.;
        %let _mono_rc=1;
        %return;
    %end;

    %if &_mono_nobs_trn.=0 %then %do;
        %put ERROR: [monotonicidad_contract] La ventana TRAIN no tiene
            observaciones en el input unificado.;
        %let _mono_rc=1;
        %return;
    %end;

    %if &_mono_nobs_oot.=0 %then %do;
        %put ERROR: [monotonicidad_contract] La ventana OOT no tiene
            observaciones en el input unificado.;
        %let _mono_rc=1;
        %return;
    %end;

    %put NOTE: [monotonicidad_contract] OK - base=&_mono_nobs_scope. obs,
        TRAIN=&_mono_nobs_trn. obs, OOT=&_mono_nobs_oot. obs.;
    %put NOTE: [monotonicidad_contract] score_var=&score_var. target=&target.
        byvar=&byvar. def_cld=&def_cld.;

%mend monotonicidad_contract;
