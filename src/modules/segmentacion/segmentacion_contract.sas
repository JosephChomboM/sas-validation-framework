/* =========================================================================
segmentacion_contract.sas - Validaciones pre-ejecucion del modulo
Segmentacion (flujo consolidado)

Verifica:
1) Tabla input accesible y con observaciones
2) Variable target definida y presente en input
3) Variable byvar definida y presente en input
4) Variable segvar definida y presente en input (WARNING si ausente)
5) Variable idvar definida y presente en input (WARNING si ausente)
6) Cobertura de TRAIN y OOT sobre el input unificado tras aplicar def_cld

Setea macro variable &_seg_rc (declarada %global por segmentacion_run):
0 = OK, 1 = fallo critico (modulo no debe ejecutarse)
========================================================================= */
%macro segmentacion_contract(input_caslib=, input_table=, target=,
    byvar=, segvar=, idvar=, def_cld=, train_min_mes=, train_max_mes=,
    oot_min_mes=, oot_max_mes=);

    %let _seg_rc = 0;

    %local _seg_table_exists _seg_has_col _seg_nobs_scope _seg_nobs_train
        _seg_nobs_oot _seg_nobs_analysis;

    %if %length(%superq(input_table)) = 0 %then %do;
        %put ERROR: [seg_contract] input_table no definida.;
        %let _seg_rc = 1;
        %return;
    %end;

    %if %length(%superq(target)) = 0 %then %do;
        %put ERROR: [seg_contract] Variable target no definida.;
        %let _seg_rc = 1;
        %return;
    %end;

    %if %length(%superq(byvar)) = 0 %then %do;
        %put ERROR: [seg_contract] Variable byvar no definida.;
        %let _seg_rc = 1;
        %return;
    %end;

    %if %length(%superq(train_min_mes)) = 0 or
        %length(%superq(train_max_mes)) = 0 or
        %length(%superq(oot_min_mes)) = 0 or
        %length(%superq(oot_max_mes)) = 0 %then %do;
        %put ERROR: [seg_contract] Ventanas TRAIN/OOT no definidas.;
        %let _seg_rc = 1;
        %return;
    %end;

    %let _seg_table_exists = 0;
    proc sql noprint;
        select count(*) into :_seg_table_exists trimmed
        from dictionary.tables
        where upcase(libname) = upcase("&input_caslib.")
          and upcase(memname) = upcase("&input_table.");
    quit;

    %if &_seg_table_exists. = 0 %then %do;
        %put ERROR: [seg_contract] &input_caslib..&input_table. no existe.;
        %let _seg_rc = 1;
        %return;
    %end;

    %let _seg_has_col = 0;
    proc sql noprint;
        select count(*) into :_seg_has_col trimmed
        from dictionary.columns
        where upcase(libname) = upcase("&input_caslib.")
          and upcase(memname) = upcase("&input_table.")
          and upcase(name) = upcase("&target.");
    quit;

    %if &_seg_has_col. = 0 %then %do;
        %put ERROR: [seg_contract] target=&target. no encontrada en input.;
        %let _seg_rc = 1;
        %return;
    %end;

    %let _seg_has_col = 0;
    proc sql noprint;
        select count(*) into :_seg_has_col trimmed
        from dictionary.columns
        where upcase(libname) = upcase("&input_caslib.")
          and upcase(memname) = upcase("&input_table.")
          and upcase(name) = upcase("&byvar.");
    quit;

    %if &_seg_has_col. = 0 %then %do;
        %put ERROR: [seg_contract] byvar=&byvar. no encontrada en input.;
        %let _seg_rc = 1;
        %return;
    %end;

    proc fedsql sessref=conn;
        create table casuser._seg_contract_counts {options replace=true} as
        select count(*) as N_Scope,
               sum(case
                       when a.&byvar. <= &def_cld.
                        and a.&byvar. >= &train_min_mes.
                        and a.&byvar. <= &train_max_mes.
                       then 1 else 0
                   end) as N_Train,
               sum(case
                       when a.&byvar. <= &def_cld.
                        and a.&byvar. >= &oot_min_mes.
                        and a.&byvar. <= &oot_max_mes.
                       then 1 else 0
                   end) as N_OOT,
               sum(case
                       when a.&byvar. <= &def_cld.
                        and (
                            (a.&byvar. >= &train_min_mes. and a.&byvar. <= &train_max_mes.)
                            or
                            (a.&byvar. >= &oot_min_mes. and a.&byvar. <= &oot_max_mes.)
                        )
                       then 1 else 0
                   end) as N_Analysis
        from &input_caslib..&input_table. a;
    quit;

    data _null_;
        set casuser._seg_contract_counts;
        call symputx('_seg_nobs_scope', N_Scope);
        call symputx('_seg_nobs_train', N_Train);
        call symputx('_seg_nobs_oot', N_OOT);
        call symputx('_seg_nobs_analysis', N_Analysis);
    run;

    proc datasets library=casuser nolist nowarn;
        delete _seg_contract_counts;
    quit;

    %if %sysevalf(%superq(_seg_nobs_scope)=, boolean) %then
        %let _seg_nobs_scope = 0;
    %if %sysevalf(%superq(_seg_nobs_train)=, boolean) %then
        %let _seg_nobs_train = 0;
    %if %sysevalf(%superq(_seg_nobs_oot)=, boolean) %then
        %let _seg_nobs_oot = 0;
    %if %sysevalf(%superq(_seg_nobs_analysis)=, boolean) %then
        %let _seg_nobs_analysis = 0;

    %if &_seg_nobs_scope. = 0 %then %do;
        %put ERROR: [seg_contract] &input_caslib..&input_table. tiene 0 obs.;
        %let _seg_rc = 1;
        %return;
    %end;

    %if &_seg_nobs_analysis. = 0 %then %do;
        %put ERROR: [seg_contract] No hay observaciones dentro de la ventana consolidada TRAIN+OOT y def_cld.;
        %let _seg_rc = 1;
        %return;
    %end;

    %if &_seg_nobs_train. = 0 %then %do;
        %put ERROR: [seg_contract] La ventana TRAIN no tiene observaciones en el input unificado.;
        %let _seg_rc = 1;
        %return;
    %end;

    %if &_seg_nobs_oot. = 0 %then %do;
        %put ERROR: [seg_contract] La ventana OOT no tiene observaciones en el input unificado.;
        %let _seg_rc = 1;
        %return;
    %end;

    %if %length(%superq(segvar)) > 0 %then %do;
        %let _seg_has_col = 0;
        proc sql noprint;
            select count(*) into :_seg_has_col trimmed
            from dictionary.columns
            where upcase(libname) = upcase("&input_caslib.")
              and upcase(memname) = upcase("&input_table.")
              and upcase(name) = upcase("&segvar.");
        quit;

        %if &_seg_has_col. = 0 %then %do;
            %put WARNING: [seg_contract] segvar=&segvar. no encontrada en input. Analisis de segmentos se omitira.;
        %end;
    %end;
    %else %do;
        %put WARNING: [seg_contract] Variable segvar no definida. Analisis de segmentos se omitira.;
    %end;

    %if %length(%superq(idvar)) > 0 %then %do;
        %let _seg_has_col = 0;
        proc sql noprint;
            select count(*) into :_seg_has_col trimmed
            from dictionary.columns
            where upcase(libname) = upcase("&input_caslib.")
              and upcase(memname) = upcase("&input_table.")
              and upcase(name) = upcase("&idvar.");
        quit;

        %if &_seg_has_col. = 0 %then %do;
            %put WARNING: [seg_contract] idvar=&idvar. no encontrada en input. Analisis de migracion se omitira.;
        %end;
    %end;
    %else %do;
        %put WARNING: [seg_contract] Variable idvar no definida. Analisis de migracion se omitira.;
    %end;

    %put NOTE: [seg_contract] OK - base=&_seg_nobs_scope. obs,
        consolidado=&_seg_nobs_analysis. obs, TRAIN=&_seg_nobs_train. obs,
        OOT=&_seg_nobs_oot. obs.;
    %put NOTE: [seg_contract] target=&target. byvar=&byvar.
        segvar=&segvar. idvar=&idvar. def_cld=&def_cld.;

%mend segmentacion_contract;
