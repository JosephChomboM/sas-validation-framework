/* =========================================================================
missings_contract.sas - Validaciones pre-ejecucion del modulo Missings

Verifica:
1) Al menos una lista de variables (num o cat) no vacia
2) Tabla TRAIN accesible y con observaciones (nobs > 0)
3) Tabla OOT accesible y con observaciones (nobs > 0)

Setea macro variable &_miss_rc (declarada %global por missings_run):
0 = OK, 1 = fallo (el modulo no debe ejecutarse)

Validacion de existencia: usa proc sql count(*) directo.
NO usa table.tableExists (no confiable en SAS Viya).
========================================================================= */
%macro missings_contract( input_caslib=, train_table=, oot_table=, vars_num=,
    vars_cat=);

    %let _miss_rc=0;

    %local _miss_nobs_trn _miss_nobs_oot;

    /* ---- 1) Validar al menos una lista de variables --------------------- */
    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then %do;
        %put ERROR: [missings_contract] No se proporcionaron variables numericas
            ni categoricas.;
        %let _miss_rc=1;
        %return;
    %end;

    /* ---- 2) Validar tabla TRAIN accesible y nobs > 0 -------------------- */
    %let _miss_nobs_trn=0;

    proc sql noprint;
        select count(*) into :_miss_nobs_trn trimmed from
            &input_caslib..&train_table.;
    quit;

    %if &_miss_nobs_trn.=0 %then %do;
        %put ERROR: [missings_contract] TRAIN &input_caslib..&train_table. no
            accesible o 0 obs.;
        %let _miss_rc=1;
        %return;
    %end;

    /* ---- 3) Validar tabla OOT accesible y nobs > 0 ---------------------- */
    %let _miss_nobs_oot=0;

    proc sql noprint;
        select count(*) into :_miss_nobs_oot trimmed from
            &input_caslib..&oot_table.;
    quit;

    %if &_miss_nobs_oot.=0 %then %do;
        %put ERROR: [missings_contract] OOT &input_caslib..&oot_table. no
            accesible o 0 obs.;
        %let _miss_rc=1;
        %return;
    %end;

    %put NOTE: [missings_contract] OK - TRAIN=&_miss_nobs_trn. obs,
        OOT=&_miss_nobs_oot. obs;
    %if %length(%superq(vars_num)) > 0 %then %put NOTE: [missings_contract]
        vars_num=&vars_num.;
    %if %length(%superq(vars_cat)) > 0 %then %put NOTE: [missings_contract]
        vars_cat=&vars_cat.;

%mend missings_contract;
