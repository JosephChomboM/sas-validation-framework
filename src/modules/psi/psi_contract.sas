/* =========================================================================
psi_contract.sas - Validaciones pre-ejecucion del modulo PSI (scope-input)

Verifica:
1) Tabla de entrada unica accesible y con observaciones.
2) Al menos una lista de variables (num o cat) no vacia.
3) byvar (si se provee) existe en input_table.
4) Variables solicitadas existen en input_table.
5) Cobertura TRAIN y OOT en input_table usando ventanas de cfg_troncales.

Setea macro variable &_psi_rc (declarada %global por psi_run):
0 = OK, 1 = fallo.

Regla: validar existencia con PROC SQL + dictionary.* o count(*) directo.
========================================================================= */

%macro _psi_contract_check_vars(input_caslib=, input_table=, vars=, label=);

    %local _i _var _exists;

    %let _i=1;
    %let _var=%scan(%superq(vars), &_i., %str( ));

    %do %while(%length(%superq(_var)) > 0);
        %let _exists=0;

        proc sql noprint;
            select count(*) into :_exists trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.")
              and upcase(name)=upcase("&_var.");
        quit;

        %if &_exists.=0 %then %do;
            %put ERROR: [psi_contract] Variable &_var. (&label.) no encontrada en &input_caslib..&input_table..;
            %let _psi_rc=1;
            %return;
        %end;

        %let _i=%eval(&_i. + 1);
        %let _var=%scan(%superq(vars), &_i., %str( ));
    %end;

%mend _psi_contract_check_vars;

%macro psi_contract(input_caslib=, input_table=, troncal_id=, vars_num=,
    vars_cat=, byvar=);

    /* _psi_rc is declared %global by psi_run (the caller). */
    %let _psi_rc=0;

    %local _psi_nobs_input _psi_has_col _cfg_byvar _train_min _train_max
        _oot_min _oot_max _psi_nobs_trn _psi_nobs_oot;

    /* ---- 1) Validar al menos una lista de variables -------------------- */
    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then %do;
        %put ERROR: [psi_contract] No se proporcionaron variables numericas ni categoricas.;
        %let _psi_rc=1;
        %return;
    %end;

    /* ---- 2) Validar input_table accesible y con nobs > 0 --------------- */
    %let _psi_nobs_input=0;

    proc sql noprint;
        select count(*) into :_psi_nobs_input trimmed
        from &input_caslib..&input_table.;
    quit;

    %if &_psi_nobs_input.=0 %then %do;
        %put ERROR: [psi_contract] Tabla de entrada &input_caslib..&input_table. no accesible o vacia.;
        %let _psi_rc=1;
        %return;
    %end;

    /* ---- 3) Validar byvar del modulo si se provee ---------------------- */
    %if %length(%superq(byvar)) > 0 %then %do;
        %let _psi_has_col=0;

        proc sql noprint;
            select count(*) into :_psi_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.")
              and upcase(name)=upcase("&byvar.");
        quit;

        %if &_psi_has_col.=0 %then %do;
            %put ERROR: [psi_contract] byvar=&byvar. no encontrada en &input_caslib..&input_table..;
            %let _psi_rc=1;
            %return;
        %end;
    %end;

    /* ---- 4) Validar variables solicitadas ------------------------------ */
    %if %length(%superq(vars_num)) > 0 %then %do;
        %_psi_contract_check_vars(input_caslib=&input_caslib.,
            input_table=&input_table., vars=&vars_num., label=NUM);
        %if &_psi_rc. ne 0 %then %return;
    %end;

    %if %length(%superq(vars_cat)) > 0 %then %do;
        %_psi_contract_check_vars(input_caslib=&input_caslib.,
            input_table=&input_table., vars=&vars_cat., label=CAT);
        %if &_psi_rc. ne 0 %then %return;
    %end;

    /* ---- 5) Resolver ventanas TRAIN/OOT desde cfg_troncales ------------ */
    %let _cfg_byvar=;
    %let _train_min=;
    %let _train_max=;
    %let _oot_min=;
    %let _oot_max=;

    proc sql noprint;
        select strip(byvar),
               strip(put(train_min_mes, best.)),
               strip(put(train_max_mes, best.)),
               strip(put(oot_min_mes, best.)),
               strip(put(oot_max_mes, best.))
          into :_cfg_byvar trimmed,
               :_train_min trimmed,
               :_train_max trimmed,
               :_oot_min trimmed,
               :_oot_max trimmed
        from casuser.cfg_troncales
        where troncal_id=&troncal_id.;
    quit;

    %if %length(%superq(_cfg_byvar))=0 or %length(%superq(_train_min))=0 or
        %length(%superq(_train_max))=0 or %length(%superq(_oot_min))=0 or
        %length(%superq(_oot_max))=0 %then %do;
        %put ERROR: [psi_contract] No se pudo resolver ventanas TRAIN/OOT desde cfg_troncales para troncal=&troncal_id..;
        %let _psi_rc=1;
        %return;
    %end;

    %let _psi_has_col=0;
    proc sql noprint;
        select count(*) into :_psi_has_col trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.")
          and upcase(name)=upcase("&_cfg_byvar.");
    quit;

    %if &_psi_has_col.=0 %then %do;
        %put ERROR: [psi_contract] byvar de cfg_troncales (&_cfg_byvar.) no existe en &input_caslib..&input_table..;
        %let _psi_rc=1;
        %return;
    %end;

    /* ---- 6) Validar cobertura TRAIN y OOT ------------------------------ */
    %let _psi_nobs_trn=0;
    %let _psi_nobs_oot=0;

    proc sql noprint;
        select count(*) into :_psi_nobs_trn trimmed
        from &input_caslib..&input_table.
        where &_cfg_byvar. >= &_train_min.
          and &_cfg_byvar. <= &_train_max.;

        select count(*) into :_psi_nobs_oot trimmed
        from &input_caslib..&input_table.
        where &_cfg_byvar. >= &_oot_min.
          and &_cfg_byvar. <= &_oot_max.;
    quit;

    %if &_psi_nobs_trn.=0 %then %do;
        %put ERROR: [psi_contract] Cobertura TRAIN vacia en &input_caslib..&input_table. (byvar=&_cfg_byvar. ventana=&_train_min.-&_train_max.).;
        %let _psi_rc=1;
        %return;
    %end;

    %if &_psi_nobs_oot.=0 %then %do;
        %put ERROR: [psi_contract] Cobertura OOT vacia en &input_caslib..&input_table. (byvar=&_cfg_byvar. ventana=&_oot_min.-&_oot_max.).;
        %let _psi_rc=1;
        %return;
    %end;

    %put NOTE: [psi_contract] OK - input=&_psi_nobs_input. TRAIN=&_psi_nobs_trn. OOT=&_psi_nobs_oot.;
    %if %length(%superq(vars_num)) > 0 %then %put NOTE: [psi_contract] vars_num=&vars_num.;
    %if %length(%superq(vars_cat)) > 0 %then %put NOTE: [psi_contract] vars_cat=&vars_cat.;
    %if %length(%superq(byvar)) > 0 %then %put NOTE: [psi_contract] byvar(modulo)=&byvar.;
    %put NOTE: [psi_contract] byvar(split)=&_cfg_byvar. TRAIN=&_train_min.-&_train_max. OOT=&_oot_min.-&_oot_max..;

%mend psi_contract;
