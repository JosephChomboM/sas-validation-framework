/* =========================================================================
estabilidad_contract.sas - Validaciones pre-ejecucion del modulo Estabilidad

Verifica:
1) Al menos una lista de variables (num o cat) no vacia
2) Variable temporal (byvar) no vacia
3) Tabla input consolidada accesible y con observaciones (nobs > 0)
4) Cobertura TRAIN y OOT en la tabla consolidada segun ventanas
5) byvar presente en input consolidado

Setea macro variable &_estab_rc (declarada %global por estabilidad_run):
0 = OK, 1 = fallo (el modulo no debe ejecutarse)

Validacion de existencia: usa proc sql count(*) directo.
NO usa table.tableExists (no confiable en SAS Viya).
========================================================================= */
%macro estabilidad_contract(input_caslib=, input_table=, vars_num=, vars_cat=,
    byvar=, train_min_mes=, train_max_mes=, oot_min_mes=, oot_max_mes=);

    %let _estab_rc=0;

    %local _estab_table_exists _estab_nobs_scope _estab_nobs_trn
        _estab_nobs_oot _estab_has_col;

    /* ---- 1) Validar al menos una lista de variables --------------------- */
    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then %do;
        %put ERROR: [estabilidad_contract] No se proporcionaron variables
            numericas ni categoricas.;
        %let _estab_rc=1;
        %return;
    %end;

    /* ---- 2) Validar byvar no vacia -------------------------------------- */
    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [estabilidad_contract] Variable temporal (byvar) no
            definida.;
        %let _estab_rc=1;
        %return;
    %end;

    /* ---- 3) Validar ventanas TRAIN/OOT ---------------------------------- */
    %if %length(%superq(train_min_mes))=0 or %length(%superq(train_max_mes))=0
        or %length(%superq(oot_min_mes))=0 or
        %length(%superq(oot_max_mes))=0 %then %do;
        %put ERROR: [estabilidad_contract] Ventanas TRAIN/OOT no definidas.;
        %let _estab_rc=1;
        %return;
    %end;

    /* ---- 4) Validar input_table existe ---------------------------------- */
    %let _estab_table_exists=0;
    proc sql noprint;
        select count(*) into :_estab_table_exists trimmed
        from dictionary.tables
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.");
    quit;

    %if &_estab_table_exists.=0 %then %do;
        %put ERROR: [estabilidad_contract] &input_caslib..&input_table. no existe.;
        %let _estab_rc=1;
        %return;
    %end;

    /* ---- 5) Validar byvar existe en input_table ------------------------- */
    %let _estab_has_col=0;

    proc sql noprint;
        select count(*) into :_estab_has_col trimmed from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.") and upcase(memname)=
            upcase("&input_table.") and upcase(name)=upcase("&byvar.");
    quit;

    %if &_estab_has_col.=0 %then %do;
        %put ERROR: [estabilidad_contract] byvar=&byvar. no encontrada en
            &input_caslib..&input_table..;
        %let _estab_rc=1;
        %return;
    %end;

    /* ---- 6) Validar cobertura de observaciones -------------------------- */
    proc fedsql sessref=conn;
        create table casuser._estab_contract_counts {options replace=true} as
        select count(*) as N_Scope,
               sum(case
                       when &byvar. >= &train_min_mes.
                        and &byvar. <= &train_max_mes.
                       then 1 else 0
                   end) as N_Train,
               sum(case
                       when &byvar. >= &oot_min_mes.
                        and &byvar. <= &oot_max_mes.
                       then 1 else 0
                   end) as N_OOT
        from &input_caslib..&input_table.;
    quit;

    data _null_;
        set casuser._estab_contract_counts;
        call symputx('_estab_nobs_scope', N_Scope);
        call symputx('_estab_nobs_trn', N_Train);
        call symputx('_estab_nobs_oot', N_OOT);
    run;

    proc datasets library=casuser nolist nowarn;
        delete _estab_contract_counts;
    quit;

    %if %sysevalf(%superq(_estab_nobs_scope)=, boolean) %then
        %let _estab_nobs_scope=0;
    %if %sysevalf(%superq(_estab_nobs_trn)=, boolean) %then
        %let _estab_nobs_trn=0;
    %if %sysevalf(%superq(_estab_nobs_oot)=, boolean) %then
        %let _estab_nobs_oot=0;

    %if &_estab_nobs_scope.=0 %then %do;
        %put ERROR: [estabilidad_contract] &input_caslib..&input_table. tiene 0 obs.;
        %let _estab_rc=1;
        %return;
    %end;

    %if &_estab_nobs_trn.=0 %then %do;
        %put ERROR: [estabilidad_contract] La ventana TRAIN no tiene observaciones en el input consolidado.;
        %let _estab_rc=1;
        %return;
    %end;

    %if &_estab_nobs_oot.=0 %then %do;
        %put ERROR: [estabilidad_contract] La ventana OOT no tiene observaciones en el input consolidado.;
        %let _estab_rc=1;
        %return;
    %end;

    %put NOTE: [estabilidad_contract] OK - base=&_estab_nobs_scope. obs,
        TRAIN=&_estab_nobs_trn. obs, OOT=&_estab_nobs_oot. obs;
    %if %length(%superq(vars_num)) > 0 %then %put NOTE: [estabilidad_contract]
        vars_num=&vars_num.;
    %if %length(%superq(vars_cat)) > 0 %then %put NOTE: [estabilidad_contract]
        vars_cat=&vars_cat.;
    %put NOTE: [estabilidad_contract] byvar=&byvar.;

%mend estabilidad_contract;
