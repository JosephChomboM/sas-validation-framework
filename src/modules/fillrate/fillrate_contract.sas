/* =========================================================================
fillrate_contract.sas - Validaciones pre-ejecucion del modulo Fillrate

Verifica:
1) Al menos una lista de variables (num o cat) no vacia
2) byvar definido
3) def_cld definido
4) target definido si hay vars_num (para fillrate vs gini)
5) input consolidado accesible y con observaciones
6) byvar existe en input consolidado
7) target existe en input consolidado si aplica
8) Cobertura TRAIN y OOT por ventanas configuradas
9) TRAIN y OOT tienen observaciones con byvar <= def_cld (bloque Gini)

Setea &_fill_rc:
0 = OK, 1 = fallo
========================================================================= */
%macro fillrate_contract(input_caslib=, input_table=, vars_num=, vars_cat=,
    byvar=, target=, def_cld=, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=);

    %let _fill_rc=0;

    %local _fill_table_exists _fill_nobs_scope _fill_nobs_trn _fill_nobs_oot
        _fill_has_col _fill_need_target _fill_nobs_trn_filt
        _fill_nobs_oot_filt;

    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then %do;
        %put ERROR: [fillrate_contract] No se proporcionaron variables
            numericas ni categoricas.;
        %let _fill_rc=1;
        %return;
    %end;

    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [fillrate_contract] Variable temporal (byvar) no definida.;
        %let _fill_rc=1;
        %return;
    %end;

    %if %length(%superq(def_cld))=0 %then %do;
        %put ERROR: [fillrate_contract] def_cld no definida.;
        %let _fill_rc=1;
        %return;
    %end;

    %let _fill_need_target=0;
    %if %length(%superq(vars_num)) > 0 %then %let _fill_need_target=1;

    %if &_fill_need_target.=1 and %length(%superq(target))=0 %then %do;
        %put ERROR: [fillrate_contract] target no definido para el calculo de
            Gini en variables numericas.;
        %let _fill_rc=1;
        %return;
    %end;

    %if %length(%superq(train_min_mes))=0 or %length(%superq(train_max_mes))=0
        or %length(%superq(oot_min_mes))=0 or
        %length(%superq(oot_max_mes))=0 %then %do;
        %put ERROR: [fillrate_contract] Ventanas TRAIN/OOT no definidas.;
        %let _fill_rc=1;
        %return;
    %end;

    %let _fill_table_exists=0;
    proc sql noprint;
        select count(*) into :_fill_table_exists trimmed
        from dictionary.tables
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.");
    quit;
    %if &_fill_table_exists.=0 %then %do;
        %put ERROR: [fillrate_contract] &input_caslib..&input_table. no existe.;
        %let _fill_rc=1;
        %return;
    %end;

    %let _fill_has_col=0;
    proc sql noprint;
        select count(*) into :_fill_has_col trimmed from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.") and
            upcase(memname)=upcase("&input_table.") and
            upcase(name)=upcase("&byvar.");
    quit;
    %if &_fill_has_col.=0 %then %do;
        %put ERROR: [fillrate_contract] byvar=&byvar. no encontrada en
            &input_caslib..&input_table..;
        %let _fill_rc=1;
        %return;
    %end;

    %if &_fill_need_target.=1 %then %do;
        %let _fill_has_col=0;
        proc sql noprint;
            select count(*) into :_fill_has_col trimmed from dictionary.columns
                where upcase(libname)=upcase("&input_caslib.") and
                upcase(memname)=upcase("&input_table.") and
                upcase(name)=upcase("&target.");
        quit;
        %if &_fill_has_col.=0 %then %do;
            %put ERROR: [fillrate_contract] target=&target. no encontrada en
                &input_caslib..&input_table..;
            %let _fill_rc=1;
            %return;
        %end;
    %end;

    proc fedsql sessref=conn;
        create table casuser._fill_contract_counts {options replace=true} as
        select count(*) as N_Scope,
               sum(case
                       when &byvar. >= &train_min_mes.
                        and &byvar. <= &train_max_mes.
                       then 1 else 0
                   end) as N_Train,
               sum(case
                       when &byvar. >= &oot_min_mes.
                        and &byvar. <= &oot_max_mes.
                       then 1 else 0
                   end) as N_OOT,
               sum(case
                       when &byvar. >= &train_min_mes.
                        and &byvar. <= &train_max_mes.
                        and &byvar. <= &def_cld.
                       then 1 else 0
                   end) as N_Train_DefCld,
               sum(case
                       when &byvar. >= &oot_min_mes.
                        and &byvar. <= &oot_max_mes.
                        and &byvar. <= &def_cld.
                       then 1 else 0
                   end) as N_OOT_DefCld
        from &input_caslib..&input_table.;
    quit;

    data _null_;
        set casuser._fill_contract_counts;
        call symputx('_fill_nobs_scope', N_Scope);
        call symputx('_fill_nobs_trn', N_Train);
        call symputx('_fill_nobs_oot', N_OOT);
        call symputx('_fill_nobs_trn_filt', N_Train_DefCld);
        call symputx('_fill_nobs_oot_filt', N_OOT_DefCld);
    run;

    proc datasets library=casuser nolist nowarn;
        delete _fill_contract_counts;
    quit;

    %if %sysevalf(%superq(_fill_nobs_scope)=, boolean) %then
        %let _fill_nobs_scope=0;
    %if %sysevalf(%superq(_fill_nobs_trn)=, boolean) %then
        %let _fill_nobs_trn=0;
    %if %sysevalf(%superq(_fill_nobs_oot)=, boolean) %then
        %let _fill_nobs_oot=0;
    %if %sysevalf(%superq(_fill_nobs_trn_filt)=, boolean) %then
        %let _fill_nobs_trn_filt=0;
    %if %sysevalf(%superq(_fill_nobs_oot_filt)=, boolean) %then
        %let _fill_nobs_oot_filt=0;

    %if &_fill_nobs_scope.=0 %then %do;
        %put ERROR: [fillrate_contract] &input_caslib..&input_table. tiene 0 obs.;
        %let _fill_rc=1;
        %return;
    %end;

    %if &_fill_nobs_trn.=0 %then %do;
        %put ERROR: [fillrate_contract] No hay cobertura TRAIN en input consolidado.;
        %let _fill_rc=1;
        %return;
    %end;

    %if &_fill_nobs_oot.=0 %then %do;
        %put ERROR: [fillrate_contract] No hay cobertura OOT en input consolidado.;
        %let _fill_rc=1;
        %return;
    %end;

    %if &_fill_nobs_trn_filt.=0 %then %do;
        %put ERROR: [fillrate_contract] TRAIN no tiene observaciones con
            &byvar. <= &def_cld.;
        %let _fill_rc=1;
        %return;
    %end;

    %if &_fill_nobs_oot_filt.=0 %then %do;
        %put ERROR: [fillrate_contract] OOT no tiene observaciones con
            &byvar. <= &def_cld.;
        %let _fill_rc=1;
        %return;
    %end;

    %put NOTE: [fillrate_contract] OK - base=&_fill_nobs_scope. obs,
        TRAIN=&_fill_nobs_trn. obs, OOT=&_fill_nobs_oot. obs.;
    %put NOTE: [fillrate_contract] TRAIN filtrado=&_fill_nobs_trn_filt.
        OOT filtrado=&_fill_nobs_oot_filt.;
    %put NOTE: [fillrate_contract] byvar=&byvar. target=&target.
        def_cld=&def_cld.;

%mend fillrate_contract;
