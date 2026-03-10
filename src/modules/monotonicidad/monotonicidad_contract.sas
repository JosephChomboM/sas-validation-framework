/* =========================================================================
monotonicidad_contract.sas - Validaciones pre-ejecucion del modulo
Monotonicidad (METOD7)

Verifica:
1) Tabla TRAIN accesible y con observaciones (nobs > 0)
2) Tabla OOT accesible y con observaciones (nobs > 0)
3) Variables requeridas no vacias: byvar, score_var, target_var, def_cld
4) Columnas requeridas existen en TRAIN y OOT
5) score_var y target_var son numericas

Setea macro variable &_mono_rc (declarada %global por monotonicidad_run):
0 = OK, 1 = fallo (el modulo no debe ejecutarse)
========================================================================= */
%macro monotonicidad_contract(input_caslib=, train_table=, oot_table=,
    byvar=, score_var=, target_var=, def_cld=);

    %let _mono_rc=0;

    %local _mono_nobs_trn _mono_nobs_oot _mono_has_col _mono_col_type;

    /* ---- 1) Validaciones de parametros obligatorios -------------------- */
    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [monotonicidad_contract] byvar no definido.;
        %let _mono_rc=1;
        %return;
    %end;

    %if %length(%superq(score_var))=0 %then %do;
        %put ERROR: [monotonicidad_contract] score_var no definido.;
        %let _mono_rc=1;
        %return;
    %end;

    %if %length(%superq(target_var))=0 %then %do;
        %put ERROR: [monotonicidad_contract] target_var no definido.;
        %let _mono_rc=1;
        %return;
    %end;

    %if %length(%superq(def_cld))=0 %then %do;
        %put ERROR: [monotonicidad_contract] def_cld no definido.;
        %let _mono_rc=1;
        %return;
    %end;

    /* ---- 2) Validar TRAIN accesible y con observaciones --------------- */
    %let _mono_nobs_trn=0;

    proc sql noprint;
        select count(*) into :_mono_nobs_trn trimmed from
            &input_caslib..&train_table.;
    quit;

    %if &_mono_nobs_trn.=0 %then %do;
        %put ERROR: [monotonicidad_contract] TRAIN &input_caslib..&train_table.
            no accesible o 0 obs.;
        %let _mono_rc=1;
        %return;
    %end;

    /* ---- 3) Validar OOT accesible y con observaciones ----------------- */
    %let _mono_nobs_oot=0;

    proc sql noprint;
        select count(*) into :_mono_nobs_oot trimmed from
            &input_caslib..&oot_table.;
    quit;

    %if &_mono_nobs_oot.=0 %then %do;
        %put ERROR: [monotonicidad_contract] OOT &input_caslib..&oot_table.
            no accesible o 0 obs.;
        %let _mono_rc=1;
        %return;
    %end;

    /* ---- 4) Validar byvar existe en TRAIN y OOT ----------------------- */
    %let _mono_has_col=0;
    proc sql noprint;
        select count(*) into :_mono_has_col trimmed from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&train_table.")
              and upcase(name)=upcase("&byvar.");
    quit;
    %if &_mono_has_col.=0 %then %do;
        %put ERROR: [monotonicidad_contract] byvar=&byvar. no existe en TRAIN.;
        %let _mono_rc=1;
        %return;
    %end;

    %let _mono_has_col=0;
    proc sql noprint;
        select count(*) into :_mono_has_col trimmed from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&oot_table.")
              and upcase(name)=upcase("&byvar.");
    quit;
    %if &_mono_has_col.=0 %then %do;
        %put ERROR: [monotonicidad_contract] byvar=&byvar. no existe en OOT.;
        %let _mono_rc=1;
        %return;
    %end;

    /* ---- 5) Validar score_var existe y es numerica -------------------- */
    %let _mono_has_col=0;
    %let _mono_col_type=;
    proc sql noprint;
        select count(*), max(upcase(type))
            into :_mono_has_col trimmed, :_mono_col_type trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&train_table.")
          and upcase(name)=upcase("&score_var.");
    quit;
    %if &_mono_has_col.=0 %then %do;
        %put ERROR: [monotonicidad_contract] score_var=&score_var.
            no existe en TRAIN.;
        %let _mono_rc=1;
        %return;
    %end;
    %if %upcase(&_mono_col_type.) ne NUM %then %do;
        %put ERROR: [monotonicidad_contract] score_var=&score_var.
            debe ser numerica.;
        %let _mono_rc=1;
        %return;
    %end;

    %let _mono_has_col=0;
    %let _mono_col_type=;
    proc sql noprint;
        select count(*), max(upcase(type))
            into :_mono_has_col trimmed, :_mono_col_type trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&oot_table.")
          and upcase(name)=upcase("&score_var.");
    quit;
    %if &_mono_has_col.=0 %then %do;
        %put ERROR: [monotonicidad_contract] score_var=&score_var.
            no existe en OOT.;
        %let _mono_rc=1;
        %return;
    %end;
    %if %upcase(&_mono_col_type.) ne NUM %then %do;
        %put ERROR: [monotonicidad_contract] score_var=&score_var.
            debe ser numerica en OOT.;
        %let _mono_rc=1;
        %return;
    %end;

    /* ---- 6) Validar target_var existe y es numerica ------------------- */
    %let _mono_has_col=0;
    %let _mono_col_type=;
    proc sql noprint;
        select count(*), max(upcase(type))
            into :_mono_has_col trimmed, :_mono_col_type trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&train_table.")
          and upcase(name)=upcase("&target_var.");
    quit;
    %if &_mono_has_col.=0 %then %do;
        %put ERROR: [monotonicidad_contract] target_var=&target_var.
            no existe en TRAIN.;
        %let _mono_rc=1;
        %return;
    %end;
    %if %upcase(&_mono_col_type.) ne NUM %then %do;
        %put ERROR: [monotonicidad_contract] target_var=&target_var.
            debe ser numerica.;
        %let _mono_rc=1;
        %return;
    %end;

    %let _mono_has_col=0;
    %let _mono_col_type=;
    proc sql noprint;
        select count(*), max(upcase(type))
            into :_mono_has_col trimmed, :_mono_col_type trimmed
        from dictionary.columns
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&oot_table.")
          and upcase(name)=upcase("&target_var.");
    quit;
    %if &_mono_has_col.=0 %then %do;
        %put ERROR: [monotonicidad_contract] target_var=&target_var.
            no existe en OOT.;
        %let _mono_rc=1;
        %return;
    %end;
    %if %upcase(&_mono_col_type.) ne NUM %then %do;
        %put ERROR: [monotonicidad_contract] target_var=&target_var.
            debe ser numerica en OOT.;
        %let _mono_rc=1;
        %return;
    %end;

    %put NOTE: [monotonicidad_contract] OK - TRAIN=&_mono_nobs_trn. obs,
        OOT=&_mono_nobs_oot. obs.;
    %put NOTE: [monotonicidad_contract] byvar=&byvar. score=&score_var.
        target=&target_var. def_cld=&def_cld.;

%mend monotonicidad_contract;
