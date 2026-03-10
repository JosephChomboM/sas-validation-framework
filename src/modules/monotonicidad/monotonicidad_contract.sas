/* =========================================================================
monotonicidad_contract.sas - Validaciones pre-ejecucion del modulo
Monotonicidad (METOD7)

Verifica:
1) Al menos una lista de variables (num o cat) no vacia
2) Variable target definida y no vacia
3) Variable temporal (byvar) definida y no vacia
4) def_cld definido y no vacio
5) Tabla TRAIN accesible y con observaciones (nobs > 0)
6) Tabla OOT accesible y con observaciones (nobs > 0)
7) byvar presente en ambas tablas
8) target presente en ambas tablas

Setea macro variable &_mono_rc (declarada %global por monotonicidad_run):
0 = OK, 1 = fallo
========================================================================= */
%macro monotonicidad_contract(input_caslib=, train_table=, oot_table=,
    vars_num=, vars_cat=, target=, byvar=, def_cld=);

    %let _mono_rc=0;

    %local _mono_nobs_trn _mono_nobs_oot _mono_has_col;

    /* ---- 1) Validar al menos una lista de variables ------------------- */
    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then %do;
        %put ERROR: [monotonicidad_contract] No se proporcionaron variables
            numericas ni categoricas.;
        %let _mono_rc=1;
        %return;
    %end;

    /* ---- 2) Validar target definido ----------------------------------- */
    %if %length(%superq(target))=0 %then %do;
        %put ERROR: [monotonicidad_contract] Variable target no definida.;
        %let _mono_rc=1;
        %return;
    %end;

    /* ---- 3) Validar byvar definido ------------------------------------ */
    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [monotonicidad_contract] Variable temporal (byvar)
            no definida.;
        %let _mono_rc=1;
        %return;
    %end;

    /* ---- 4) Validar def_cld definido ---------------------------------- */
    %if %length(%superq(def_cld))=0 %then %do;
        %put ERROR: [monotonicidad_contract] def_cld no definido.;
        %let _mono_rc=1;
        %return;
    %end;

    /* ---- 5) Validar tabla TRAIN accesible y nobs > 0 ------------------ */
    %let _mono_nobs_trn=0;
    proc sql noprint;
        select count(*) into :_mono_nobs_trn trimmed
        from &input_caslib..&train_table.;
    quit;

    %if &_mono_nobs_trn.=0 %then %do;
        %put ERROR: [monotonicidad_contract] TRAIN &input_caslib..&train_table.
            no accesible o 0 obs.;
        %let _mono_rc=1;
        %return;
    %end;

    /* ---- 6) Validar tabla OOT accesible y nobs > 0 -------------------- */
    %let _mono_nobs_oot=0;
    proc sql noprint;
        select count(*) into :_mono_nobs_oot trimmed
        from &input_caslib..&oot_table.;
    quit;

    %if &_mono_nobs_oot.=0 %then %do;
        %put ERROR: [monotonicidad_contract] OOT &input_caslib..&oot_table.
            no accesible o 0 obs.;
        %let _mono_rc=1;
        %return;
    %end;

    /* ---- 7) Validar byvar en TRAIN y OOT ------------------------------ */
    %let _mono_has_col=0;
    proc sql noprint;
        select count(*) into :_mono_has_col trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&train_table.")
          and upcase(name)=upcase("&byvar.");
    quit;
    %if &_mono_has_col.=0 %then %do;
        %put ERROR: [monotonicidad_contract] byvar=&byvar. no encontrada en TRAIN.;
        %let _mono_rc=1;
        %return;
    %end;

    %let _mono_has_col=0;
    proc sql noprint;
        select count(*) into :_mono_has_col trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&oot_table.")
          and upcase(name)=upcase("&byvar.");
    quit;
    %if &_mono_has_col.=0 %then %do;
        %put ERROR: [monotonicidad_contract] byvar=&byvar. no encontrada en OOT.;
        %let _mono_rc=1;
        %return;
    %end;

    /* ---- 8) Validar target en TRAIN y OOT ----------------------------- */
    %let _mono_has_col=0;
    proc sql noprint;
        select count(*) into :_mono_has_col trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&train_table.")
          and upcase(name)=upcase("&target.");
    quit;
    %if &_mono_has_col.=0 %then %do;
        %put ERROR: [monotonicidad_contract] target=&target. no encontrada en TRAIN.;
        %let _mono_rc=1;
        %return;
    %end;

    %let _mono_has_col=0;
    proc sql noprint;
        select count(*) into :_mono_has_col trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&oot_table.")
          and upcase(name)=upcase("&target.");
    quit;
    %if &_mono_has_col.=0 %then %do;
        %put ERROR: [monotonicidad_contract] target=&target. no encontrada en OOT.;
        %let _mono_rc=1;
        %return;
    %end;

    %put NOTE: [monotonicidad_contract] OK - TRAIN=&_mono_nobs_trn. obs,
        OOT=&_mono_nobs_oot. obs;
    %if %length(%superq(vars_num)) > 0 %then %put NOTE: [monotonicidad_contract]
        vars_num=&vars_num.;
    %if %length(%superq(vars_cat)) > 0 %then %put NOTE: [monotonicidad_contract]
        vars_cat=&vars_cat.;
    %put NOTE: [monotonicidad_contract] target=&target. byvar=&byvar.
        def_cld=&def_cld.;

%mend monotonicidad_contract;
