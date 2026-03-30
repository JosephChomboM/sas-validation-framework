/* =========================================================================
missings_contract.sas - Validaciones pre-ejecucion del modulo Missings

Modos soportados:
- DERIVED    : tabla unificada + byvar + ventanas cfg_troncales
- PRELABELED : tabla ya normalizada con columna Split (compat legacy)

Setea macro variable &_miss_rc (declarada %global por missings_run):
0 = OK, 1 = fallo
========================================================================= */
%macro missings_contract(input_caslib=, input_table=, split_mode=DERIVED,
    split_var=Split, byvar=, train_min_mes=, train_max_mes=, oot_min_mes=,
    oot_max_mes=, vars_num=, vars_cat=);

    %let _miss_rc=0;

    %local _miss_table_exists _miss_has_col _miss_nobs_scope _miss_nobs_trn
        _miss_nobs_oot _miss_missing_cols _miss_split_col;

    %macro _miss_validate_var_list(list=, list_name=);
        %local _i _v _has_col;

        %let _i=1;
        %let _v=%scan(%superq(list), &_i., %str( ));
        %do %while(%length(%superq(_v)) > 0);
            %let _has_col=0;
            proc sql noprint;
                select count(*) into :_has_col trimmed
                from dictionary.columns
                where upcase(libname)=upcase("&input_caslib.")
                  and upcase(memname)=upcase("&input_table.")
                  and upcase(name)=upcase("&_v.");
            quit;

            %if &_has_col.=0 %then %do;
                %let _miss_missing_cols=&_miss_missing_cols. &_v.;
                %put ERROR: [missings_contract] &_v. no existe en
                    &input_caslib..&input_table. (lista=&list_name.).;
            %end;

            %let _i=%eval(&_i. + 1);
            %let _v=%scan(%superq(list), &_i., %str( ));
        %end;
    %mend _miss_validate_var_list;

    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then %do;
        %put ERROR: [missings_contract] No se proporcionaron variables numericas
            ni categoricas.;
        %let _miss_rc=1;
        %return;
    %end;

    %if %length(%superq(input_table))=0 %then %do;
        %put ERROR: [missings_contract] input_table no definida.;
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

    %let _miss_nobs_scope=0;
    proc sql noprint;
        select count(*) into :_miss_nobs_scope trimmed
        from &input_caslib..&input_table.;
    quit;

    %if &_miss_nobs_scope.=0 %then %do;
        %put ERROR: [missings_contract] &input_caslib..&input_table. tiene 0 obs.;
        %let _miss_rc=1;
        %return;
    %end;

    %let _miss_missing_cols=;

    %if %length(%superq(vars_num)) > 0 %then
        %_miss_validate_var_list(list=&vars_num., list_name=NUM);

    %if %length(%superq(vars_cat)) > 0 %then
        %_miss_validate_var_list(list=&vars_cat., list_name=CAT);

    %if %length(%superq(_miss_missing_cols)) > 0 %then %do;
        %put ERROR: [missings_contract] Variables faltantes:&_miss_missing_cols.;
        %let _miss_rc=1;
        %return;
    %end;

    %if %upcase(&split_mode.)=DERIVED %then %do;
        %if %length(%superq(byvar))=0 %then %do;
            %put ERROR: [missings_contract] byvar no definida para split_mode=DERIVED.;
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

        proc fedsql sessref=conn;
            create table casuser._miss_contract_counts {options replace=true} as
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
    %end;
    %else %if %upcase(&split_mode.)=PRELABELED %then %do;
        %if %length(%superq(split_var))=0 %then %do;
            %put ERROR: [missings_contract] split_var no definida para split_mode=PRELABELED.;
            %let _miss_rc=1;
            %return;
        %end;

        %let _miss_split_col=0;
        proc sql noprint;
            select count(*) into :_miss_split_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.")
              and upcase(name)=upcase("&split_var.");
        quit;

        %if &_miss_split_col.=0 %then %do;
            %put ERROR: [missings_contract] split_var=&split_var. no encontrada
                en &input_caslib..&input_table..;
            %let _miss_rc=1;
            %return;
        %end;

        proc fedsql sessref=conn;
            create table casuser._miss_contract_counts {options replace=true} as
            select count(*) as N_Scope,
                   sum(case when upcase(&split_var.)='TRAIN' then 1 else 0 end)
                       as N_Train,
                   sum(case when upcase(&split_var.)='OOT' then 1 else 0 end)
                       as N_OOT
            from &input_caslib..&input_table.;
        quit;
    %end;
    %else %do;
        %put ERROR: [missings_contract] split_mode=&split_mode. no reconocido.;
        %let _miss_rc=1;
        %return;
    %end;

    data _null_;
        set casuser._miss_contract_counts;
        call symputx('_miss_nobs_scope', N_Scope);
        call symputx('_miss_nobs_trn', N_Train);
        call symputx('_miss_nobs_oot', N_OOT);
    run;

    proc datasets library=casuser nolist nowarn;
        delete _miss_contract_counts;
    quit;

    %if %sysevalf(%superq(_miss_nobs_trn)=, boolean) %then %let _miss_nobs_trn=0;
    %if %sysevalf(%superq(_miss_nobs_oot)=, boolean) %then %let _miss_nobs_oot=0;

    %if &_miss_nobs_trn.=0 %then %do;
        %put ERROR: [missings_contract] La cobertura TRAIN quedo vacia en
            &input_caslib..&input_table..;
        %let _miss_rc=1;
        %return;
    %end;

    %if &_miss_nobs_oot.=0 %then %do;
        %put ERROR: [missings_contract] La cobertura OOT quedo vacia en
            &input_caslib..&input_table..;
        %let _miss_rc=1;
        %return;
    %end;

    %put NOTE: [missings_contract] OK - mode=&split_mode. base=&_miss_nobs_scope.
        TRAIN=&_miss_nobs_trn. OOT=&_miss_nobs_oot.;
    %if %length(%superq(vars_num)) > 0 %then %put NOTE: [missings_contract]
        vars_num=&vars_num.;
    %if %length(%superq(vars_cat)) > 0 %then %put NOTE: [missings_contract]
        vars_cat=&vars_cat.;

%mend missings_contract;
