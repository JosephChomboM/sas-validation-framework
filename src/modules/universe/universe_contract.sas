/* =========================================================================
universe_contract.sas - Validaciones pre-ejecución del módulo Universe

Verifica:
1) Tabla TRAIN accesible y con observaciones (nobs > 0)
2) Tabla OOT accesible y con observaciones (nobs > 0)
3) Variable temporal (byvar) existe en ambas tablas
4) Variable ID (id_var) existe en ambas tablas
5) Variable monto (opcional) → solo WARNING si no existe

Setea macro variable &_univ_rc (declarada %global por universe_run):
0 = OK, 1 = fallo (el módulo no debe ejecutarse)
========================================================================= */
%macro universe_contract( input_caslib=, train_table=, oot_table=, byvar=,
    id_var=, monto_var=);

    %let _univ_rc=0;

    %local _univ_nobs_trn _univ_nobs_oot _univ_has_col;

    /* ---- 1) Validar byvar no vacía --------------------------------------- */
    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [universe_contract] Variable temporal (byvar) no definida.;
        %let _univ_rc=1;
        %return;
    %end;

    /* ---- 2) Validar id_var no vacía -------------------------------------- */
    %if %length(%superq(id_var))=0 %then %do;
        %put ERROR: [universe_contract] Variable ID (id_var) no definida.;
        %let _univ_rc=1;
        %return;
    %end;

    /* ---- 3) Validar tabla TRAIN accesible y nobs > 0 --------------------- */
    %let _univ_nobs_trn=0;

    proc sql noprint;
        select count(*) into :_univ_nobs_trn trimmed from
            &input_caslib..&train_table.;
    quit;

    %if &_univ_nobs_trn.=0 %then %do;
        %put ERROR: [universe_contract] TRAIN &input_caslib..&train_table. no
            accesible o 0 obs.;
        %let _univ_rc=1;
        %return;
    %end;

    /* ---- 4) Validar tabla OOT accesible y nobs > 0 ----------------------- */
    %let _univ_nobs_oot=0;

    proc sql noprint;
        select count(*) into :_univ_nobs_oot trimmed from
            &input_caslib..&oot_table.;
    quit;

    %if &_univ_nobs_oot.=0 %then %do;
        %put ERROR: [universe_contract] OOT &input_caslib..&oot_table. no
            accesible o 0 obs.;
        %let _univ_rc=1;
        %return;
    %end;

    /* ---- 5) Validar byvar existe en ambas tablas ------------------------- */
    %let _univ_has_col=0;

    proc sql noprint;
        select count(*) into :_univ_has_col trimmed from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.") and upcase(memname)=
            upcase("&train_table.") and upcase(name)=upcase("&byvar.");
    quit;
    %if &_univ_has_col.=0 %then %do;
        %put ERROR: [universe_contract] byvar=&byvar. no encontrada en TRAIN.;
        %let _univ_rc=1;
        %return;
    %end;

    /* ---- 6) Validar id_var existe en ambas tablas ------------------------ */
    %let _univ_has_col=0;

    proc sql noprint;
        select count(*) into :_univ_has_col trimmed from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.") and upcase(memname)=
            upcase("&train_table.") and upcase(name)=upcase("&id_var.");
    quit;
    %if &_univ_has_col.=0 %then %do;
        %put ERROR: [universe_contract] id_var=&id_var. no encontrada en TRAIN.;
        %let _univ_rc=1;
        %return;
    %end;

    /* ---- 7) Monto: solo WARNING si no existe ----------------------------- */
    %if %length(%superq(monto_var)) > 0 %then %do;
        %let _univ_has_col=0;

        proc sql noprint;
            select count(*) into :_univ_has_col trimmed from dictionary.columns
                where upcase(libname)=upcase("&input_caslib.") and
                upcase(memname)=upcase("&train_table.") and upcase(name)=
                upcase("&monto_var.");
        quit;
        %if &_univ_has_col.=0 %then %put WARNING: [universe_contract]
            monto_var=&monto_var. no encontrada. Se omitirán análisis de monto.;
    %end;
    %else %do;
        %put WARNING: [universe_contract] monto_var no definida. Se omitirán
            análisis de monto.;
    %end;

    %put NOTE: [universe_contract] OK - TRAIN=&_univ_nobs_trn. obs,
        OOT=&_univ_nobs_oot. obs;
    %put NOTE: [universe_contract] byvar=&byvar. id_var=&id_var.
        monto_var=&monto_var.;

%mend universe_contract;
