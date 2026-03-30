/* =========================================================================
missings_contract.sas - Validaciones pre-ejecucion del modulo Missings

Valida:
1) Existe input
2) Hay al menos una lista de variables
3) Existen byvar / split segun modo
4) Existen variables solicitadas
5) Hay cobertura TRAIN y OOT

Setea &_miss_rc:
0 = OK
1 = fallo
========================================================================= */
%macro _miss_contract_validate_vars(input_caslib=, input_table=, vars=);

    %local _i _var _has_col;

    %let _i=1;
    %let _var=%scan(%superq(vars), &_i., %str( ));
    %do %while(%length(%superq(_var)) > 0);
        %let _has_col=0;
        proc sql noprint;
            select count(*) into :_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.")
              and upcase(name)=upcase("&_var.");
        quit;

        %if &_has_col.=0 %then %do;
            %put ERROR: [missings_contract] Variable &_var. no encontrada en
                &input_caslib..&input_table..;
            %let _miss_rc=1;
            %return;
        %end;

        %let _i=%eval(&_i. + 1);
        %let _var=%scan(%superq(vars), &_i., %str( ));
    %end;

%mend _miss_contract_validate_vars;

%macro missings_contract(input_caslib=, input_table=, split_mode=DERIVED,
    split_var=Split, byvar=, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=, vars_num=, vars_cat=);

    %global _miss_rc;
    %let _miss_rc=0;

    %local _miss_table_exists _miss_nobs_scope _miss_nobs_train
        _miss_nobs_oot _miss_has_col;

    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then
        %do;
        %put ERROR: [missings_contract] No se proporcionaron variables
            numericas ni categoricas.;
        %let _miss_rc=1;
        %return;
    %end;

    %let _miss_table_exists=0;
    proc sql noprint;
        select count(*) into :_miss_table_exists trimmed
        from dictionary.tables
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.");
    quit;

    %if &_miss_table_exists.=0 %then %do;
        %put ERROR: [missings_contract] &input_caslib..&input_table. no existe.;
        %let _miss_rc=1;
        %return;
    %end;

    proc sql noprint;
        select count(*) into :_miss_nobs_scope trimmed
        from &input_caslib..&input_table.;
    quit;

    %if %sysevalf(%superq(_miss_nobs_scope)=, boolean) %then
        %let _miss_nobs_scope=0;

    %if &_miss_nobs_scope.=0 %then %do;
        %put ERROR: [missings_contract] &input_caslib..&input_table. tiene 0
            observaciones.;
        %let _miss_rc=1;
        %return;
    %end;

    %if %upcase(&split_mode.)=DERIVED %then %do;

        %if %length(%superq(byvar))=0 %then %do;
            %put ERROR: [missings_contract] byvar no definida para split
                derivado.;
            %let _miss_rc=1;
            %return;
        %end;

        %if %length(%superq(train_min_mes))=0 or
            %length(%superq(train_max_mes))=0 or
            %length(%superq(oot_min_mes))=0 or
            %length(%superq(oot_max_mes))=0 %then %do;
            %put ERROR: [missings_contract] Ventanas TRAIN/OOT no definidas.;
            %let _miss_rc=1;
            %return;
        %end;

        %let _miss_has_col=0;
        proc sql noprint;
            select count(*) into :_miss_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.")
              and upcase(name)=upcase("&byvar.");
        quit;

        %if &_miss_has_col.=0 %then %do;
            %put ERROR: [missings_contract] byvar=&byvar. no encontrada en
                &input_caslib..&input_table..;
            %let _miss_rc=1;
            %return;
        %end;

    %end;
    %else %if %upcase(&split_mode.)=PRELABELED %then %do;

        %if %length(%superq(split_var))=0 %then %do;
            %put ERROR: [missings_contract] split_var no definida para input
                pre-etiquetado.;
            %let _miss_rc=1;
            %return;
        %end;

        %let _miss_has_col=0;
        proc sql noprint;
            select count(*) into :_miss_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.")
              and upcase(name)=upcase("&split_var.");
        quit;

        %if &_miss_has_col.=0 %then %do;
            %put ERROR: [missings_contract] split_var=&split_var. no encontrada
                en &input_caslib..&input_table..;
            %let _miss_rc=1;
            %return;
        %end;

    %end;
    %else %do;
        %put ERROR: [missings_contract] split_mode=&split_mode. no reconocido.;
        %let _miss_rc=1;
        %return;
    %end;

    %_miss_contract_validate_vars(input_caslib=&input_caslib.,
        input_table=&input_table., vars=&vars_num.);
    %if &_miss_rc. ne 0 %then %return;

    %_miss_contract_validate_vars(input_caslib=&input_caslib.,
        input_table=&input_table., vars=&vars_cat.);
    %if &_miss_rc. ne 0 %then %return;

    %if %upcase(&split_mode.)=DERIVED %then %do;
        proc fedsql sessref=conn;
            create table casuser._miss_contract_counts {options replace=true} as
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
                       end) as N_OOT
            from &input_caslib..&input_table.;
        quit;
    %end;
    %else %do;
        proc fedsql sessref=conn;
            create table casuser._miss_contract_counts {options replace=true} as
            select count(*) as N_Scope,
                   sum(case
                           when upcase(&split_var.)='TRAIN'
                           then 1 else 0
                       end) as N_Train,
                   sum(case
                           when upcase(&split_var.)='OOT'
                           then 1 else 0
                       end) as N_OOT
            from &input_caslib..&input_table.;
        quit;
    %end;

    data _null_;
        set casuser._miss_contract_counts;
        call symputx('_miss_nobs_scope', N_Scope);
        call symputx('_miss_nobs_train', N_Train);
        call symputx('_miss_nobs_oot', N_OOT);
    run;

    proc datasets library=casuser nolist nowarn;
        delete _miss_contract_counts;
    quit;

    %if %sysevalf(%superq(_miss_nobs_train)=, boolean) %then
        %let _miss_nobs_train=0;
    %if %sysevalf(%superq(_miss_nobs_oot)=, boolean) %then
        %let _miss_nobs_oot=0;

    %if &_miss_nobs_train.=0 %then %do;
        %put ERROR: [missings_contract] No hay cobertura TRAIN.;
        %let _miss_rc=1;
        %return;
    %end;

    %if &_miss_nobs_oot.=0 %then %do;
        %put ERROR: [missings_contract] No hay cobertura OOT.;
        %let _miss_rc=1;
        %return;
    %end;

    %put NOTE: [missings_contract] OK - scope=&_miss_nobs_scope. TRAIN=
        &_miss_nobs_train. OOT=&_miss_nobs_oot..;

%mend missings_contract;
