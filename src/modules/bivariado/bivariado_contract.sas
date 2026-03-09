/* =========================================================================
bivariado_contract.sas - Validaciones pre-ejecucion del modulo Bivariado

Verifica:
1) Al menos una lista de variables (num o cat) no vacia
2) Variable target definida y no vacia
3) Tabla TRAIN accesible y con observaciones (nobs > 0)
4) Tabla OOT accesible y con observaciones (nobs > 0)
5) Variable target existe en ambas tablas

Setea macro variable &_biv_rc (declarada %global por bivariado_run):
0 = OK, 1 = fallo (el modulo no debe ejecutarse)

Validacion de existencia: usa proc sql count(*) directo.
NO usa table.tableExists (no confiable en SAS Viya).
========================================================================= */
%macro bivariado_contract( input_caslib=, train_table=, oot_table=, vars_num=,
    vars_cat=, target=);

    %let _biv_rc=0;

    %local _biv_nobs_trn _biv_nobs_oot _biv_has_col;

    /* ---- 1) Validar al menos una lista de variables --------------------- */
    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then %do;
        %put ERROR: [bivariado_contract] No se proporcionaron variables
            numericas ni categoricas.;
        %let _biv_rc=1;
        %return;
    %end;

    /* ---- 2) Validar target definido ------------------------------------- */
    %if %length(%superq(target))=0 %then %do;
        %put ERROR: [bivariado_contract] Variable target no definida.;
        %let _biv_rc=1;
        %return;
    %end;

    /* ---- 3) Validar tabla TRAIN accesible y nobs > 0 -------------------- */
    %let _biv_nobs_trn=0;

    proc sql noprint;
        select count(*) into :_biv_nobs_trn trimmed from
            &input_caslib..&train_table.;
    quit;

    %if &_biv_nobs_trn.=0 %then %do;
        %put ERROR: [bivariado_contract] TRAIN &input_caslib..&train_table. no
            accesible o 0 obs.;
        %let _biv_rc=1;
        %return;
    %end;

    /* ---- 4) Validar tabla OOT accesible y nobs > 0 ---------------------- */
    %let _biv_nobs_oot=0;

    proc sql noprint;
        select count(*) into :_biv_nobs_oot trimmed from
            &input_caslib..&oot_table.;
    quit;

    %if &_biv_nobs_oot.=0 %then %do;
        %put ERROR: [bivariado_contract] OOT &input_caslib..&oot_table. no
            accesible o 0 obs.;
        %let _biv_rc=1;
        %return;
    %end;

    /* ---- 5) Validar target existe en TRAIN ------------------------------ */
    %let _biv_has_col=0;

    proc sql noprint;
        select count(*) into :_biv_has_col trimmed from dictionary.columns where
            upcase(libname)=upcase("&input_caslib.") and upcase(memname)=
            upcase("&train_table.") and upcase(name)=upcase("&target.");
    quit;

    %if &_biv_has_col.=0 %then %do;
        %put ERROR: [bivariado_contract] target=&target. no encontrada en
            TRAIN.;
        %let _biv_rc=1;
        %return;
    %end;

    /* ---- Validar target existe en OOT ----------------------------------- */
    %let _biv_has_col=0;

    proc sql noprint;
        select count(*) into :_biv_has_col trimmed from dictionary.columns where
            upcase(libname)=upcase("&input_caslib.") and upcase(memname)=
            upcase("&oot_table.") and upcase(name)=upcase("&target.");
    quit;

    %if &_biv_has_col.=0 %then %do;
        %put ERROR: [bivariado_contract] target=&target. no encontrada en OOT.;
        %let _biv_rc=1;
        %return;
    %end;

    %put NOTE: [bivariado_contract] OK - TRAIN=&_biv_nobs_trn. obs,
        OOT=&_biv_nobs_oot. obs;
    %if %length(%superq(vars_num)) > 0 %then %put NOTE: [bivariado_contract]
        vars_num=&vars_num.;
    %if %length(%superq(vars_cat)) > 0 %then %put NOTE: [bivariado_contract]
        vars_cat=&vars_cat.;
    %put NOTE: [bivariado_contract] target=&target.;

%mend bivariado_contract;
