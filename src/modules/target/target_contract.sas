/* =========================================================================
target_contract.sas - Validaciones pre-ejecucion del modulo Target

Verifica:
1) Variable target no vacia
2) Variable temporal (byvar) no vacia
3) def_cld definido y no vacio
4) Tabla TRAIN accesible y con observaciones (nobs > 0)
5) Tabla OOT accesible y con observaciones (nobs > 0)
6) byvar presente en ambas tablas
7) target presente en ambas tablas
8) monto (opcional) - solo WARNING si no existe

Setea macro variable &_tgt_rc (declarada %global por target_run):
0 = OK, 1 = fallo (el modulo no debe ejecutarse)
========================================================================= */
%macro target_contract( input_caslib=, train_table=, oot_table=, target=,
    byvar=, monto_var=, def_cld=);

    %let _tgt_rc=0;

    %local _tgt_nobs_trn _tgt_nobs_oot _tgt_has_col;

    /* ---- 1) Validar target no vacia -------------------------------------- */
    %if %length(%superq(target))=0 %then %do;
        %put ERROR: [target_contract] Variable target no definida.;
        %let _tgt_rc=1;
        %return;
    %end;

    /* ---- 2) Validar byvar no vacia --------------------------------------- */
    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [target_contract] Variable temporal (byvar) no definida.;
        %let _tgt_rc=1;
        %return;
    %end;

    /* ---- 3) Validar def_cld definido ------------------------------------- */
    %if %length(%superq(def_cld))=0 %then %do;
        %put ERROR: [target_contract] def_cld (fecha cierre default) no
            definida.;
        %let _tgt_rc=1;
        %return;
    %end;

    /* ---- 4) Validar tabla TRAIN accesible y nobs > 0 --------------------- */
    %let _tgt_nobs_trn=0;

    proc sql noprint;
        select count(*) into :_tgt_nobs_trn trimmed from
            &input_caslib..&train_table.;
    quit;

    %if &_tgt_nobs_trn.=0 %then %do;
        %put ERROR: [target_contract] TRAIN &input_caslib..&train_table. no
            accesible o 0 obs.;
        %let _tgt_rc=1;
        %return;
    %end;

    /* ---- 5) Validar tabla OOT accesible y nobs > 0 ----------------------- */
    %let _tgt_nobs_oot=0;

    proc sql noprint;
        select count(*) into :_tgt_nobs_oot trimmed from
            &input_caslib..&oot_table.;
    quit;

    %if &_tgt_nobs_oot.=0 %then %do;
        %put ERROR: [target_contract] OOT &input_caslib..&oot_table. no
            accesible o 0 obs.;
        %let _tgt_rc=1;
        %return;
    %end;

    /* ---- 6) Validar byvar existe en ambas tablas ------------------------- */
    %let _tgt_has_col=0;

    proc sql noprint;
        select count(*) into :_tgt_has_col trimmed from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.") and upcase(memname)=
            upcase("&train_table.") and upcase(name)=upcase("&byvar.");
    quit;

    %if &_tgt_has_col.=0 %then %do;
        %put ERROR: [target_contract] byvar=&byvar. no encontrada en TRAIN.;
        %let _tgt_rc=1;
        %return;
    %end;

    %let _tgt_has_col=0;

    proc sql noprint;
        select count(*) into :_tgt_has_col trimmed from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.") and upcase(memname)=
            upcase("&oot_table.") and upcase(name)=upcase("&byvar.");
    quit;

    %if &_tgt_has_col.=0 %then %do;
        %put ERROR: [target_contract] byvar=&byvar. no encontrada en OOT.;
        %let _tgt_rc=1;
        %return;
    %end;

    /* ---- 7) Validar target existe en ambas tablas ------------------------ */
    %let _tgt_has_col=0;

    proc sql noprint;
        select count(*) into :_tgt_has_col trimmed from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.") and upcase(memname)=
            upcase("&train_table.") and upcase(name)=upcase("&target.");
    quit;

    %if &_tgt_has_col.=0 %then %do;
        %put ERROR: [target_contract] target=&target. no encontrada en TRAIN.;
        %let _tgt_rc=1;
        %return;
    %end;

    %let _tgt_has_col=0;

    proc sql noprint;
        select count(*) into :_tgt_has_col trimmed from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.") and upcase(memname)=
            upcase("&oot_table.") and upcase(name)=upcase("&target.");
    quit;

    %if &_tgt_has_col.=0 %then %do;
        %put ERROR: [target_contract] target=&target. no encontrada en OOT.;
        %let _tgt_rc=1;
        %return;
    %end;

    /* ---- 8) Monto: solo WARNING si no existe ----------------------------- */
    %if %length(%superq(monto_var)) > 0 %then %do;
        %let _tgt_has_col=0;

        proc sql noprint;
            select count(*) into :_tgt_has_col trimmed from dictionary.columns
                where upcase(libname)=upcase("&input_caslib.") and
                upcase(memname)=upcase("&train_table.") and upcase(name)=
                upcase("&monto_var.");
        quit;

        %if &_tgt_has_col.=0 %then %put WARNING: [target_contract]
            monto_var=&monto_var. no encontrada. Se omitiran analisis
            ponderados.;
    %end;
    %else %do;
        %put WARNING: [target_contract] monto_var no definida. Se omitiran
            analisis ponderados.;
    %end;

    %put NOTE: [target_contract] OK - TRAIN=&_tgt_nobs_trn. obs,
        OOT=&_tgt_nobs_oot. obs;
    %put NOTE: [target_contract] target=&target. byvar=&byvar. def_cld=&def_cld.
        monto=&monto_var.;

%mend target_contract;
