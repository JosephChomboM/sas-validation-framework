/* =========================================================================
replica_contract.sas - Validaciones pre-ejecucion del modulo Replica

Verifica:
1) Lista de variables no vacia
2) target definido
3) byvar definido
4) def_cld definido
5) TRAIN accesible y con observaciones
6) OOT accesible y con observaciones
7) byvar presente en TRAIN y OOT
8) target presente en TRAIN y OOT
9) time_var (opcional) presente en TRAIN y OOT
10) control_var (opcional) presente en TRAIN y OOT
11) variables de lista presentes en TRAIN y OOT

Setea macro variable &_rep_rc:
0 = OK, 1 = fallo
========================================================================= */
%macro replica_contract(input_caslib=, train_table=, oot_table=,
    lista_variables=, target=, byvar=, def_cld=, time_var=, control_var=);

    %let _rep_rc=0;

    %local _rep_nobs_trn _rep_nobs_oot _rep_has_col _rep_n_vars _i _var_i;

    /* ---- 1) Lista de variables --------------------------------------- */
    %if %length(%superq(lista_variables))=0 %then %do;
        %put ERROR: [replica_contract] No se proporcionaron variables.;
        %let _rep_rc=1;
        %return;
    %end;

    /* ---- 2) target ---------------------------------------------------- */
    %if %length(%superq(target))=0 %then %do;
        %put ERROR: [replica_contract] target no definido.;
        %let _rep_rc=1;
        %return;
    %end;

    /* ---- 3) byvar ----------------------------------------------------- */
    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [replica_contract] byvar no definido.;
        %let _rep_rc=1;
        %return;
    %end;

    /* ---- 4) def_cld --------------------------------------------------- */
    %if %length(%superq(def_cld))=0 %then %do;
        %put ERROR: [replica_contract] def_cld no definido.;
        %let _rep_rc=1;
        %return;
    %end;

    /* ---- 5) TRAIN accesible ------------------------------------------ */
    %let _rep_nobs_trn=0;
    proc sql noprint;
        select count(*) into :_rep_nobs_trn trimmed
        from &input_caslib..&train_table.;
    quit;

    %if &_rep_nobs_trn.=0 %then %do;
        %put ERROR: [replica_contract] TRAIN &input_caslib..&train_table.
            no accesible o 0 obs.;
        %let _rep_rc=1;
        %return;
    %end;

    /* ---- 6) OOT accesible -------------------------------------------- */
    %let _rep_nobs_oot=0;
    proc sql noprint;
        select count(*) into :_rep_nobs_oot trimmed
        from &input_caslib..&oot_table.;
    quit;

    %if &_rep_nobs_oot.=0 %then %do;
        %put ERROR: [replica_contract] OOT &input_caslib..&oot_table.
            no accesible o 0 obs.;
        %let _rep_rc=1;
        %return;
    %end;

    /* ---- 7) byvar en TRAIN/OOT --------------------------------------- */
    %let _rep_has_col=0;
    proc sql noprint;
        select count(*) into :_rep_has_col trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&train_table.")
          and upcase(name)=upcase("&byvar.");
    quit;
    %if &_rep_has_col.=0 %then %do;
        %put ERROR: [replica_contract] byvar=&byvar. no encontrada en TRAIN.;
        %let _rep_rc=1;
        %return;
    %end;

    %let _rep_has_col=0;
    proc sql noprint;
        select count(*) into :_rep_has_col trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&oot_table.")
          and upcase(name)=upcase("&byvar.");
    quit;
    %if &_rep_has_col.=0 %then %do;
        %put ERROR: [replica_contract] byvar=&byvar. no encontrada en OOT.;
        %let _rep_rc=1;
        %return;
    %end;

    /* ---- 8) target en TRAIN/OOT -------------------------------------- */
    %let _rep_has_col=0;
    proc sql noprint;
        select count(*) into :_rep_has_col trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&train_table.")
          and upcase(name)=upcase("&target.");
    quit;
    %if &_rep_has_col.=0 %then %do;
        %put ERROR: [replica_contract] target=&target. no encontrado en TRAIN.;
        %let _rep_rc=1;
        %return;
    %end;

    %let _rep_has_col=0;
    proc sql noprint;
        select count(*) into :_rep_has_col trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&oot_table.")
          and upcase(name)=upcase("&target.");
    quit;
    %if &_rep_has_col.=0 %then %do;
        %put ERROR: [replica_contract] target=&target. no encontrado en OOT.;
        %let _rep_rc=1;
        %return;
    %end;

    /* ---- 9) time_var (opcional) -------------------------------------- */
    %if %length(%superq(time_var)) > 0 %then %do;
        %let _rep_has_col=0;
        proc sql noprint;
            select count(*) into :_rep_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&train_table.")
              and upcase(name)=upcase("&time_var.");
        quit;
        %if &_rep_has_col.=0 %then %do;
            %put ERROR: [replica_contract] time_var=&time_var. no encontrado
                en TRAIN.;
            %let _rep_rc=1;
            %return;
        %end;

        %let _rep_has_col=0;
        proc sql noprint;
            select count(*) into :_rep_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&oot_table.")
              and upcase(name)=upcase("&time_var.");
        quit;
        %if &_rep_has_col.=0 %then %do;
            %put ERROR: [replica_contract] time_var=&time_var. no encontrado
                en OOT.;
            %let _rep_rc=1;
            %return;
        %end;
    %end;

    /* ---- 10) control_var (opcional) ---------------------------------- */
    %if %length(%superq(control_var)) > 0 %then %do;
        %let _rep_has_col=0;
        proc sql noprint;
            select count(*) into :_rep_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&train_table.")
              and upcase(name)=upcase("&control_var.");
        quit;
        %if &_rep_has_col.=0 %then %do;
            %put ERROR: [replica_contract] control_var=&control_var. no
                encontrado en TRAIN.;
            %let _rep_rc=1;
            %return;
        %end;

        %let _rep_has_col=0;
        proc sql noprint;
            select count(*) into :_rep_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&oot_table.")
              and upcase(name)=upcase("&control_var.");
        quit;
        %if &_rep_has_col.=0 %then %do;
            %put ERROR: [replica_contract] control_var=&control_var. no
                encontrado en OOT.;
            %let _rep_rc=1;
            %return;
        %end;
    %end;

    /* ---- 11) Variables numericas en TRAIN y OOT ---------------------- */
    %let _rep_n_vars=%sysfunc(countw(&lista_variables.));

    %do _i=1 %to &_rep_n_vars.;
        %let _var_i=%scan(&lista_variables., &_i.);

        %let _rep_has_col=0;
        proc sql noprint;
            select count(*) into :_rep_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&train_table.")
              and upcase(name)=upcase("&_var_i.");
        quit;
        %if &_rep_has_col.=0 %then %do;
            %put ERROR: [replica_contract] Variable &_var_i. no encontrada
                en TRAIN.;
            %let _rep_rc=1;
            %return;
        %end;

        %let _rep_has_col=0;
        proc sql noprint;
            select count(*) into :_rep_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&oot_table.")
              and upcase(name)=upcase("&_var_i.");
        quit;
        %if &_rep_has_col.=0 %then %do;
            %put ERROR: [replica_contract] Variable &_var_i. no encontrada
                en OOT.;
            %let _rep_rc=1;
            %return;
        %end;
    %end;

    %put NOTE: [replica_contract] OK - TRAIN=&_rep_nobs_trn. obs,
        OOT=&_rep_nobs_oot. obs.;
    %put NOTE: [replica_contract] target=&target. byvar=&byvar.
        time_var=&time_var. control_var=&control_var. def_cld=&def_cld.;

%mend replica_contract;
