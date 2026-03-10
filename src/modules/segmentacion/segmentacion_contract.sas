/* =========================================================================
segmentacion_contract.sas - Validaciones pre-ejecucion del modulo
Segmentacion

Verifica:
1) Tabla input accesible y con observaciones (nobs > 0)
2) Variable target definida y presente en input
3) Variable byvar definida y presente en input
4) Variable segvar definida y presente en input (WARNING si ausente)
5) Variable idvar definida y presente en input (WARNING si ausente)

Setea macro variable &_seg_rc (declarada %global por segmentacion_run):
0 = OK, 1 = fallo critico (modulo no debe ejecutarse)

Nota: segvar e idvar generan WARNING (no ERROR) porque el modulo
puede ejecutar materialidad global sin segmentos.
========================================================================= */
%macro segmentacion_contract(input_caslib=, input_table=, target=,
    byvar=, segvar=, idvar=);

    %let _seg_rc = 0;

    %local _seg_nobs _seg_has_col;

    /* ---- 1) Validar tabla accesible y nobs > 0 ------------------------ */
    %let _seg_nobs = 0;

    proc sql noprint;
        select count(*) into :_seg_nobs trimmed
        from &input_caslib..&input_table.;
    quit;

    %if &_seg_nobs. = 0 %then %do;
        %put ERROR: [seg_contract] Tabla &input_caslib..&input_table.
            no accesible o 0 obs.;
        %let _seg_rc = 1;
        %return;
    %end;

    /* ---- 2) Validar target definido y presente ------------------------ */
    %if %length(%superq(target)) = 0 %then %do;
        %put ERROR: [seg_contract] Variable target no definida.;
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

    /* ---- 3) Validar byvar definido y presente ------------------------- */
    %if %length(%superq(byvar)) = 0 %then %do;
        %put ERROR: [seg_contract] Variable byvar no definida.;
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

    /* ---- 4) Validar segvar (WARNING si ausente) ----------------------- */
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
            %put WARNING: [seg_contract] segvar=&segvar. no encontrada
                en input. Analisis de segmentos se omitira.;
        %end;
    %end;
    %else %do;
        %put WARNING: [seg_contract] Variable segvar no definida.
            Analisis de segmentos se omitira.;
    %end;

    /* ---- 5) Validar idvar (WARNING si ausente) ------------------------ */
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
            %put WARNING: [seg_contract] idvar=&idvar. no encontrada
                en input. Analisis de migracion se omitira.;
        %end;
    %end;
    %else %do;
        %put WARNING: [seg_contract] Variable idvar no definida.
            Analisis de migracion se omitira.;
    %end;

    %put NOTE: [seg_contract] Validaciones OK -
        nobs=&_seg_nobs. target=&target. byvar=&byvar.
        segvar=&segvar. idvar=&idvar.;

%mend segmentacion_contract;
