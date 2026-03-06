/* =========================================================================
psi_contract.sas - Validaciones pre-ejecución del módulo PSI

Verifica:
1) Tabla TRAIN accesible y con observaciones (nobs > 0)
2) Tabla OOT accesible y con observaciones (nobs > 0)
3) Al menos una lista de variables (num o cat) no vacía
4) Variable temporal (byvar) existe en ambas tablas (si se proporcionó)

Setea macro variable &_psi_rc (declarada %global por psi_run):
0 = OK, 1 = fallo (el módulo no debe ejecutarse)

Validación de existencia: usa proc sql count(*) directo.
NO usa table.tableExists (no confiable en SAS Viya).
========================================================================= */
%macro psi_contract( input_caslib=, train_table=, oot_table=, vars_num=,
    vars_cat=, byvar=);

    /* _psi_rc is declared %global by psi_run (the caller).
    Do NOT re-declare %global here. */
    %let _psi_rc=0;

    %local _psi_nobs_trn _psi_nobs_oot _psi_has_byvar;

    /* ---- 1) Validar al menos una lista de variables ---------------------- */
    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then %do;
        %put ERROR: [psi_contract] No se proporcionaron variables numéricas ni
            categóricas.;
        %let _psi_rc=1;
        %return;
    %end;

    /* ---- 2) Validar tabla TRAIN accesible y nobs > 0 --------------------- */
    %let _psi_nobs_trn=0;

    proc sql noprint;
        select count(*) into :_psi_nobs_trn trimmed from
            &input_caslib..&train_table.;
    quit;

    %if &_psi_nobs_trn.=0 %then %do;
        %put ERROR: [psi_contract] Tabla TRAIN &input_caslib..&train_table. no
            accesible o tiene 0 obs.;
        %let _psi_rc=1;
        %return;
    %end;

    /* ---- 3) Validar tabla OOT accesible y nobs > 0 ----------------------- */
    %let _psi_nobs_oot=0;

    proc sql noprint;
        select count(*) into :_psi_nobs_oot trimmed from
            &input_caslib..&oot_table.;
    quit;

    %if &_psi_nobs_oot.=0 %then %do;
        %put ERROR: [psi_contract] Tabla OOT &input_caslib..&oot_table. no
            accesible o tiene 0 obs.;
        %let _psi_rc=1;
        %return;
    %end;

    /* ---- 4) Validar variable temporal (byvar) si se proporcionó ---------- */
    %if %length(%superq(byvar)) > 0 %then %do;
        %let _psi_has_byvar=0;

        proc sql noprint;
            select count(*) into :_psi_has_byvar trimmed from dictionary.columns
                where upcase(libname)=upcase("&input_caslib.") and
                upcase(memname)=upcase("&train_table.") and upcase(name)=
                upcase("&byvar.");
        quit;

        %if &_psi_has_byvar.=0 %then %do;
            %put ERROR: [psi_contract] Variable temporal &byvar. no encontrada
                en TRAIN &input_caslib..&train_table..;
            %let _psi_rc=1;
            %return;
        %end;

        %let _psi_has_byvar=0;

        proc sql noprint;
            select count(*) into :_psi_has_byvar trimmed from dictionary.columns
                where upcase(libname)=upcase("&input_caslib.") and
                upcase(memname)=upcase("&oot_table.") and upcase(name)=
                upcase("&byvar.");
        quit;

        %if &_psi_has_byvar.=0 %then %do;
            %put ERROR: [psi_contract] Variable temporal &byvar. no encontrada
                en OOT &input_caslib..&oot_table..;
            %let _psi_rc=1;
            %return;
        %end;
    %end;

    %put NOTE: [psi_contract] OK - TRAIN=&_psi_nobs_trn. obs,
        OOT=&_psi_nobs_oot. obs;
    %if %length(%superq(vars_num)) > 0 %then %put NOTE: [psi_contract]
        vars_num=&vars_num.;
    %if %length(%superq(vars_cat)) > 0 %then %put NOTE: [psi_contract]
        vars_cat=&vars_cat.;
    %if %length(%superq(byvar)) > 0 %then %put NOTE: [psi_contract]
        byvar=&byvar.;

%mend psi_contract;
