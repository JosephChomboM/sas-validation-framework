/* =========================================================================
similitud_contract.sas - Validaciones pre-ejecucion del modulo Similitud

Valida:
1) Al menos una lista de variables (num o cat) no vacia
2) target, byvar y split_var definidos
3) Tabla unificada accesible y con observaciones
4) Columnas requeridas presentes (target, byvar, split_var)
5) Cobertura de observaciones en Split=TRAIN y Split=OOT
========================================================================= */
%macro similitud_contract(input_caslib=, input_table=, vars_num=, vars_cat=,
    target=, byvar=, split_var=Split);

    %let _simil_rc=0;

    %local _simil_table_exists _simil_nobs_scope _simil_nobs_train
        _simil_nobs_oot _simil_has_col;

    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then
        %do;
        %put ERROR: [similitud_contract] No se proporcionaron variables numericas ni categoricas.;
        %let _simil_rc=1;
        %return;
    %end;

    %if %length(%superq(target))=0 %then %do;
        %put ERROR: [similitud_contract] target no definido.;
        %let _simil_rc=1;
        %return;
    %end;

    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [similitud_contract] byvar no definido.;
        %let _simil_rc=1;
        %return;
    %end;

    %if %length(%superq(split_var))=0 %then %do;
        %put ERROR: [similitud_contract] split_var no definido.;
        %let _simil_rc=1;
        %return;
    %end;

    %if %length(%superq(input_table))=0 %then %do;
        %put ERROR: [similitud_contract] input_table no definido.;
        %let _simil_rc=1;
        %return;
    %end;

    %let _simil_table_exists=0;
    proc sql noprint;
        select count(*) into :_simil_table_exists trimmed
        from dictionary.tables
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.");
    quit;

    %if &_simil_table_exists.=0 %then %do;
        %put ERROR: [similitud_contract] &input_caslib..&input_table. no existe.;
        %let _simil_rc=1;
        %return;
    %end;

    proc sql noprint;
        select count(*) into :_simil_nobs_scope trimmed
        from &input_caslib..&input_table.;
    quit;

    %if &_simil_nobs_scope.=0 %then %do;
        %put ERROR: [similitud_contract] &input_caslib..&input_table. tiene 0 obs.;
        %let _simil_rc=1;
        %return;
    %end;

    %macro _simil_chk_col(col=, label=);
        %let _simil_has_col=0;
        proc sql noprint;
            select count(*) into :_simil_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.")
              and upcase(name)=upcase("&col.");
        quit;

        %if &_simil_has_col.=0 %then %do;
            %put ERROR: [similitud_contract] &label.=&col. no encontrada en &input_table..;
            %let _simil_rc=1;
            %return;
        %end;
    %mend _simil_chk_col;

    %_simil_chk_col(col=&target., label=target);
    %if &_simil_rc. ne 0 %then %return;
    %_simil_chk_col(col=&byvar., label=byvar);
    %if &_simil_rc. ne 0 %then %return;
    %_simil_chk_col(col=&split_var., label=split_var);
    %if &_simil_rc. ne 0 %then %return;

    proc sql noprint;
        select count(*) into :_simil_nobs_train trimmed
        from &input_caslib..&input_table.
        where upcase(strip(&split_var.))='TRAIN';

        select count(*) into :_simil_nobs_oot trimmed
        from &input_caslib..&input_table.
        where upcase(strip(&split_var.))='OOT';
    quit;

    %if &_simil_nobs_train.=0 %then %do;
        %put ERROR: [similitud_contract] No hay observaciones TRAIN en &input_caslib..&input_table..;
        %let _simil_rc=1;
        %return;
    %end;

    %if &_simil_nobs_oot.=0 %then %do;
        %put ERROR: [similitud_contract] No hay observaciones OOT en &input_caslib..&input_table..;
        %let _simil_rc=1;
        %return;
    %end;

    %put NOTE: [similitud_contract] OK - scope=&_simil_nobs_scope. obs, TRAIN=&_simil_nobs_train. obs, OOT=&_simil_nobs_oot. obs.;
    %if %length(%superq(vars_num)) > 0 %then
        %put NOTE: [similitud_contract] vars_num=&vars_num.;
    %if %length(%superq(vars_cat)) > 0 %then
        %put NOTE: [similitud_contract] vars_cat=&vars_cat.;
    %put NOTE: [similitud_contract] target=&target. byvar=&byvar. split_var=&split_var.;

%mend similitud_contract;
