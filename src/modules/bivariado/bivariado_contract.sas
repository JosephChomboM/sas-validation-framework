/* =========================================================================
bivariado_contract.sas - Validaciones pre-ejecucion del modulo Bivariado

Soporta dos modos por compatibilidad:
1) input_table=  -> flujo unificado sobre _scope_input
2) train_table=/oot_table= -> flujo legacy dual-input

En el modo unificado valida:
- al menos una lista de variables (num o cat) no vacia
- target y byvar definidos y presentes
- existencia del input unificado
- cobertura TRAIN/OOT segun ventanas de cfg_troncales
========================================================================= */
%macro bivariado_contract(input_caslib=, input_table=, train_table=,
    oot_table=, vars_num=, vars_cat=, target=, byvar=, train_min_mes=,
    train_max_mes=, oot_min_mes=, oot_max_mes=, def_cld=);

    %let _biv_rc=0;

    %local _biv_table_exists _biv_has_col _biv_nobs_scope _biv_nobs_trn
        _biv_nobs_oot;

    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then %do;
        %put ERROR: [bivariado_contract] No se proporcionaron variables numericas ni categoricas.;
        %let _biv_rc=1;
        %return;
    %end;

    %if %length(%superq(target))=0 %then %do;
        %put ERROR: [bivariado_contract] Variable target no definida.;
        %let _biv_rc=1;
        %return;
    %end;

    %if %length(%superq(input_table)) > 0 %then %do;
        %if %length(%superq(byvar))=0 %then %do;
            %put ERROR: [bivariado_contract] Variable byvar no definida para el flujo unificado.;
            %let _biv_rc=1;
            %return;
        %end;

        %if %length(%superq(train_min_mes))=0 or %length(%superq(train_max_mes))=0 or
            %length(%superq(oot_min_mes))=0 or %length(%superq(oot_max_mes))=0 or
            %length(%superq(def_cld))=0 %then %do;
            %put ERROR: [bivariado_contract] Ventanas TRAIN/OOT o def_cld no definidos.;
            %let _biv_rc=1;
            %return;
        %end;

        %let _biv_table_exists=0;
        proc sql noprint;
            select count(*) into :_biv_table_exists trimmed
            from dictionary.tables
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.");
        quit;

        %if &_biv_table_exists.=0 %then %do;
            %put ERROR: [bivariado_contract] &input_caslib..&input_table. no existe.;
            %let _biv_rc=1;
            %return;
        %end;

        %let _biv_has_col=0;
        proc sql noprint;
            select count(*) into :_biv_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.")
              and upcase(name)=upcase("&target.");
        quit;

        %if &_biv_has_col.=0 %then %do;
            %put ERROR: [bivariado_contract] target=&target. no encontrada en el input unificado.;
            %let _biv_rc=1;
            %return;
        %end;

        %let _biv_has_col=0;
        proc sql noprint;
            select count(*) into :_biv_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.")
              and upcase(name)=upcase("&byvar.");
        quit;

        %if &_biv_has_col.=0 %then %do;
            %put ERROR: [bivariado_contract] byvar=&byvar. no encontrada en el input unificado.;
            %let _biv_rc=1;
            %return;
        %end;

        proc fedsql sessref=conn;
            create table casuser._biv_contract_counts {options replace=true} as
            select count(*) as N_Scope,
                   sum(case
                           when &byvar. <= &def_cld.
                            and &byvar. >= &train_min_mes.
                            and &byvar. <= &train_max_mes.
                           then 1 else 0
                       end) as N_Train,
                   sum(case
                           when &byvar. <= &def_cld.
                            and &byvar. >= &oot_min_mes.
                            and &byvar. <= &oot_max_mes.
                           then 1 else 0
                       end) as N_OOT
            from &input_caslib..&input_table.;
        quit;

        data _null_;
            set casuser._biv_contract_counts;
            call symputx('_biv_nobs_scope', N_Scope);
            call symputx('_biv_nobs_trn', N_Train);
            call symputx('_biv_nobs_oot', N_OOT);
        run;

        proc datasets library=casuser nolist nowarn;
            delete _biv_contract_counts;
        quit;

        %if %sysevalf(%superq(_biv_nobs_scope)=, boolean) %then %let _biv_nobs_scope=0;
        %if %sysevalf(%superq(_biv_nobs_trn)=, boolean) %then %let _biv_nobs_trn=0;
        %if %sysevalf(%superq(_biv_nobs_oot)=, boolean) %then %let _biv_nobs_oot=0;

        %if &_biv_nobs_scope.=0 %then %do;
            %put ERROR: [bivariado_contract] &input_caslib..&input_table. tiene 0 obs.;
            %let _biv_rc=1;
            %return;
        %end;

        %if &_biv_nobs_trn.=0 %then %do;
            %put ERROR: [bivariado_contract] La ventana TRAIN no tiene observaciones en el input unificado.;
            %let _biv_rc=1;
            %return;
        %end;

        %if &_biv_nobs_oot.=0 %then %do;
            %put ERROR: [bivariado_contract] La ventana OOT no tiene observaciones en el input unificado.;
            %let _biv_rc=1;
            %return;
        %end;

        %put NOTE: [bivariado_contract] OK - base=&_biv_nobs_scope. obs,
            TRAIN=&_biv_nobs_trn. obs, OOT=&_biv_nobs_oot. obs.;
    %end;
    %else %do;
        /* Compatibilidad legacy dual-input */
        %if %length(%superq(train_table))=0 or %length(%superq(oot_table))=0 %then %do;
            %put ERROR: [bivariado_contract] Se requiere input_table= o bien train_table=/oot_table=.;
            %let _biv_rc=1;
            %return;
        %end;

        %let _biv_nobs_trn=0;
        proc sql noprint;
            select count(*) into :_biv_nobs_trn trimmed
            from &input_caslib..&train_table.;
        quit;

        %if &_biv_nobs_trn.=0 %then %do;
            %put ERROR: [bivariado_contract] TRAIN &input_caslib..&train_table. no accesible o 0 obs.;
            %let _biv_rc=1;
            %return;
        %end;

        %let _biv_nobs_oot=0;
        proc sql noprint;
            select count(*) into :_biv_nobs_oot trimmed
            from &input_caslib..&oot_table.;
        quit;

        %if &_biv_nobs_oot.=0 %then %do;
            %put ERROR: [bivariado_contract] OOT &input_caslib..&oot_table. no accesible o 0 obs.;
            %let _biv_rc=1;
            %return;
        %end;

        %let _biv_has_col=0;
        proc sql noprint;
            select count(*) into :_biv_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&train_table.")
              and upcase(name)=upcase("&target.");
        quit;

        %if &_biv_has_col.=0 %then %do;
            %put ERROR: [bivariado_contract] target=&target. no encontrada en TRAIN.;
            %let _biv_rc=1;
            %return;
        %end;

        %let _biv_has_col=0;
        proc sql noprint;
            select count(*) into :_biv_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&oot_table.")
              and upcase(name)=upcase("&target.");
        quit;

        %if &_biv_has_col.=0 %then %do;
            %put ERROR: [bivariado_contract] target=&target. no encontrada en OOT.;
            %let _biv_rc=1;
            %return;
        %end;

        %put NOTE: [bivariado_contract] OK - compatibilidad legacy TRAIN=&_biv_nobs_trn. obs, OOT=&_biv_nobs_oot. obs.;
    %end;

    %if %length(%superq(vars_num)) > 0 %then %put NOTE: [bivariado_contract] vars_num=&vars_num.;
    %if %length(%superq(vars_cat)) > 0 %then %put NOTE: [bivariado_contract] vars_cat=&vars_cat.;
    %put NOTE: [bivariado_contract] target=&target. byvar=&byvar.;

%mend bivariado_contract;
