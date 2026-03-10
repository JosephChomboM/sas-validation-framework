/* =========================================================================
bootstrap_contract.sas - Validaciones pre-ejecucion del modulo Bootstrap

Verifica:
1) Lista de variables no vacia
2) Variable target definida y no vacia
3) Tabla TRAIN accesible y con observaciones (nobs > 0)
4) Tabla OOT accesible y con observaciones (nobs > 0)
5) target presente en TRAIN
6) target presente en OOT
7) Todas las variables de la lista presentes en TRAIN

Setea macro variable &_boot_rc (declarada %global por bootstrap_run):
0 = OK, 1 = fallo (el modulo no debe ejecutarse)

Validacion de existencia: usa proc sql count(*) directo.
NO usa table.tableExists (no confiable en SAS Viya).
========================================================================= */
%macro bootstrap_contract(input_caslib=, train_table=, oot_table=,
    lista_variables=, target=);

    %let _boot_rc = 0;

    %local _boot_nobs_trn _boot_nobs_oot _boot_has_col
        _v _boot_n_vars _boot_var_i;

    /* ---- 1) Validar lista de variables no vacia ----------------------- */
    %if %length(%superq(lista_variables)) = 0 %then %do;
        %put ERROR: [bootstrap_contract] No se proporcionaron variables.;
        %let _boot_rc = 1;
        %return;
    %end;

    /* ---- 2) Validar target definido ----------------------------------- */
    %if %length(%superq(target)) = 0 %then %do;
        %put ERROR: [bootstrap_contract] Variable target no definida.;
        %let _boot_rc = 1;
        %return;
    %end;

    /* ---- 3) Validar tabla TRAIN accesible y nobs > 0 ------------------ */
    %let _boot_nobs_trn = 0;

    proc sql noprint;
        select count(*) into :_boot_nobs_trn trimmed
        from &input_caslib..&train_table.;
    quit;

    %if &_boot_nobs_trn. = 0 %then %do;
        %put ERROR: [bootstrap_contract] TRAIN &input_caslib..&train_table.
            no accesible o 0 obs.;
        %let _boot_rc = 1;
        %return;
    %end;

    /* ---- 4) Validar tabla OOT accesible y nobs > 0 -------------------- */
    %let _boot_nobs_oot = 0;

    proc sql noprint;
        select count(*) into :_boot_nobs_oot trimmed
        from &input_caslib..&oot_table.;
    quit;

    %if &_boot_nobs_oot. = 0 %then %do;
        %put ERROR: [bootstrap_contract] OOT &input_caslib..&oot_table.
            no accesible o 0 obs.;
        %let _boot_rc = 1;
        %return;
    %end;

    /* ---- 5) Validar target presente en TRAIN -------------------------- */
    %let _boot_has_col = 0;

    proc sql noprint;
        select count(*) into :_boot_has_col trimmed
        from dictionary.columns
        where upcase(libname) = upcase("&input_caslib.")
          and upcase(memname) = upcase("&train_table.")
          and upcase(name) = upcase("&target.");
    quit;

    %if &_boot_has_col. = 0 %then %do;
        %put ERROR: [bootstrap_contract] target=&target. no encontrada
            en TRAIN.;
        %let _boot_rc = 1;
        %return;
    %end;

    /* ---- 6) Validar target presente en OOT ---------------------------- */
    %let _boot_has_col = 0;

    proc sql noprint;
        select count(*) into :_boot_has_col trimmed
        from dictionary.columns
        where upcase(libname) = upcase("&input_caslib.")
          and upcase(memname) = upcase("&oot_table.")
          and upcase(name) = upcase("&target.");
    quit;

    %if &_boot_has_col. = 0 %then %do;
        %put ERROR: [bootstrap_contract] target=&target. no encontrada
            en OOT.;
        %let _boot_rc = 1;
        %return;
    %end;

    /* ---- 7) Validar variables presentes en TRAIN ---------------------- */
    %let _boot_n_vars = %sysfunc(countw(&lista_variables.));

    %do _v = 1 %to &_boot_n_vars.;
        %let _boot_var_i = %scan(&lista_variables., &_v.);
        %let _boot_has_col = 0;

        proc sql noprint;
            select count(*) into :_boot_has_col trimmed
            from dictionary.columns
            where upcase(libname) = upcase("&input_caslib.")
              and upcase(memname) = upcase("&train_table.")
              and upcase(name) = upcase("&_boot_var_i.");
        quit;

        %if &_boot_has_col. = 0 %then %do;
            %put ERROR: [bootstrap_contract] Variable &_boot_var_i.
                no encontrada en TRAIN.;
            %let _boot_rc = 1;
            %return;
        %end;
    %end;

    %put NOTE: [bootstrap_contract] Validaciones OK -
        TRAIN nobs=&_boot_nobs_trn. OOT nobs=&_boot_nobs_oot.
        vars=%sysfunc(countw(&lista_variables.)) target=&target.;

%mend bootstrap_contract;
