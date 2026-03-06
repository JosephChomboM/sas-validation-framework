/* =========================================================================
   psi_compute.sas - Cálculo del PSI (Population Stability Index)

   Macros:
     %_psi_calc    - PSI para UNA variable entre dev y oot (core)
     %_psi_compute - Orquestador: itera variables × periodos, genera cubo + resumen

   Output (tablas en work):
     _psi_cubo       - detalle: Variable × Periodo × PSI × Tipo
     _psi_cubo_wide  - pivot:   Variable × mes_1 ... mes_N × PSI_Total
     _psi_resumen    - resumen: Variable × PSI_Total × estadísticas × alertas

   Migrado desde psi_legacy.sas v2.3 (Joseph Chombo, 02/12/2025).
   Adaptado a framework: prefijos _psi_, work library, parámetros explícitos.
   ========================================================================= */

/* ------------------------------------------------------------------
   %_psi_calc - PSI para una variable individual

   Método: PROC RANK (continuas) o valores directos (categóricas)
   + frecuencias vía PROC SQL + suavizado Laplace.
   Resultado en macro variable global &_psi_valor.
   ------------------------------------------------------------------ */
%macro _psi_calc(dev=, oot=, var=, n_buckets=10, flg_continue=1);

    %local rnd n_cortes i tot_dev tot_oot;
    %let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));

    %global _psi_valor;
    %let _psi_valor = 0;

    /* === Variables continuas: crear buckets con PROC RANK === */
    %if &flg_continue. = 1 %then %do;

        proc rank data=&dev.(keep=&var.) out=_rk_d_&rnd. groups=&n_buckets.;
            var &var.;
            ranks bucket;
        run;

        proc sql noprint;
            create table _ct_&rnd. as
            select bucket, max(&var.) as corte
            from _rk_d_&rnd.
            where bucket is not missing
            group by bucket
            order by bucket;

            select count(*) into :n_cortes trimmed from _ct_&rnd.;
        quit;

        %if &n_cortes. > 0 %then %do;
            proc sql noprint;
                select corte into :corte1 - :corte&n_cortes. from _ct_&rnd.;
            quit;

            /* Asignar buckets a DEV usando cortes */
            data _db_&rnd.;
                set &dev.(keep=&var.);
                if missing(&var.) then bucket = 0;
                %do i = 1 %to &n_cortes.;
                    %if &i. = 1 %then %do;
                        else if &var. <= &&corte&i. then bucket = &i.;
                    %end;
                    %else %if &i. = &n_cortes. %then %do;
                        else bucket = &i.;
                    %end;
                    %else %do;
                        else if &var. <= &&corte&i. then bucket = &i.;
                    %end;
                %end;
            run;

            /* Asignar buckets a OOT usando mismos cortes */
            data _ob_&rnd.;
                set &oot.(keep=&var.);
                if missing(&var.) then bucket = 0;
                %do i = 1 %to &n_cortes.;
                    %if &i. = 1 %then %do;
                        else if &var. <= &&corte&i. then bucket = &i.;
                    %end;
                    %else %if &i. = &n_cortes. %then %do;
                        else bucket = &i.;
                    %end;
                    %else %do;
                        else if &var. <= &&corte&i. then bucket = &i.;
                    %end;
                %end;
            run;
        %end;
        %else %do;
            /* Sin cortes válidos - un solo bucket */
            data _db_&rnd.; set &dev.(keep=&var.); bucket = 1; run;
            data _ob_&rnd.; set &oot.(keep=&var.); bucket = 1; run;
        %end;

    %end;
    /* === Variables categóricas: valor como bucket === */
    %else %do;
        data _db_&rnd.; set &dev.(keep=&var.); bucket = &var.; run;
        data _ob_&rnd.; set &oot.(keep=&var.); bucket = &var.; run;
    %end;

    /* === Frecuencias y PSI con SQL (suavizado Laplace) === */
    proc sql noprint;
        create table _fd_&rnd. as
        select bucket, count(*) as n_dev from _db_&rnd. group by bucket;

        create table _fo_&rnd. as
        select bucket, count(*) as n_oot from _ob_&rnd. group by bucket;

        select coalesce(sum(n_dev), 0) into :tot_dev trimmed from _fd_&rnd.;
        select coalesce(sum(n_oot), 0) into :tot_oot trimmed from _fo_&rnd.;
    quit;

    %if &tot_dev. > 0 and &tot_oot. > 0 %then %do;
        proc sql noprint;
            create table _pc_&rnd. as
            select
                coalesce(a.bucket, b.bucket) as bucket,
                (coalesce(a.n_dev, 0) + 0.5) / (&tot_dev. + 0.5 * &n_buckets.) as pct_dev,
                (coalesce(b.n_oot, 0) + 0.5) / (&tot_oot. + 0.5 * &n_buckets.) as pct_oot
            from _fd_&rnd. a
            full join _fo_&rnd. b on a.bucket = b.bucket;

            select sum((pct_oot - pct_dev) * log(pct_oot / pct_dev))
                into :_psi_valor trimmed
            from _pc_&rnd.;
        quit;
    %end;

    %if %length(&_psi_valor.) = 0 or &_psi_valor. = . %then %let _psi_valor = 0;

    /* Cleanup temporales de _psi_calc */
    proc datasets lib=work nolist nowarn;
        delete _rk_d_&rnd. _ct_&rnd. _db_&rnd. _ob_&rnd.
               _fd_&rnd. _fo_&rnd. _pc_&rnd.;
    quit;

%mend _psi_calc;


/* ------------------------------------------------------------------
   %_psi_compute - Orquestador PSI

   1) Copia datos a work (_psi_dev, _psi_oot)
   2) Construye lista combinada de variables (num + cat con marcador #)
   3) Obtiene periodos del OOT (vía byvar)
   4) Itera: PSI Mensual + PSI Total por variable
   5) Genera cubo wide (pivot Variable × Periodo)
   6) Genera resumen con estadísticas y alertas
   7) Limpia tablas intermedias (dev, oot, oot_mes)

   Output en work: _psi_cubo, _psi_cubo_wide, _psi_resumen
   ------------------------------------------------------------------ */
%macro _psi_compute(
    train_data   =,
    oot_data     =,
    byvar        =,
    var_num_list =,
    var_cat_list =,
    n_buckets    = 10,
    mensual      = 1
);

    %local n v v_aux m c z v_cat es_categorica num_valores
           meses_oot n_meses hay_mensual _combined_vars;

    %put NOTE: [psi_compute] Inicio - buckets=&n_buckets. mensual=&mensual.;

    /* ==================================================================
       1) Copiar datos a work para procesamiento local
       ================================================================== */
    data _psi_dev; set &train_data.; run;
    data _psi_oot; set &oot_data.; run;

    /* ==================================================================
       2) Construir lista combinada: numéricas plain + categóricas con #
       ================================================================== */
    %let _combined_vars = &var_num_list.;

    %let z = 1;
    %let v_cat = %scan(&var_cat_list., &z., %str( ));
    %do %while(%length(&v_cat.) > 0);
        %let _combined_vars = &_combined_vars. &v_cat.#;
        %let z = %eval(&z. + 1);
        %let v_cat = %scan(&var_cat_list., &z., %str( ));
    %end;

    %put NOTE: [psi_compute] Variables combinadas: &_combined_vars.;

    /* ==================================================================
       3) Obtener lista de periodos en OOT (si byvar proporcionado)
       ================================================================== */
    %let meses_oot = ;
    %let n_meses   = 0;

    %if %length(%superq(byvar)) > 0 %then %do;
        proc sql noprint;
            select distinct &byvar. into :meses_oot separated by ' '
            from _psi_oot
            order by &byvar.;

            select count(distinct &byvar.) into :n_meses trimmed
            from _psi_oot;
        quit;
        %put NOTE: [psi_compute] Periodos en OOT: &meses_oot. (total=&n_meses.);
    %end;
    %else %do;
        %put NOTE: [psi_compute] Sin variable temporal - solo PSI Total.;
        %let mensual = 0;
    %end;

    /* ==================================================================
       4) Inicializar CUBO PSI
       ================================================================== */
    proc sql;
        create table _psi_cubo (
            Variable  char(45),
            Periodo   num,
            PSI       num format=10.6,
            Tipo      char(15)
        );
    quit;

    /* ==================================================================
       5) Calcular PSI por variable × periodo
       ================================================================== */
    %let n = 1;
    %let v = %scan(&_combined_vars., &n., %str( ));

    %do %while(%length(&v.) > 0);

        /* ---- Detectar si es categórica ---- */
        %let es_categorica = 0;
        %let v_aux = &v.;

        %if %index(&v., %str(#)) > 0 %then %do;
            /* Explícitamente categórica (marcada con #) */
            %let v_aux = %substr(&v., 1, %eval(%length(&v.) - 1));
            %let es_categorica = 1;
        %end;
        %else %do;
            /* Heurística: ≤ 10 valores distintos → categórica */
            proc sql noprint;
                select count(distinct &v_aux.) into :num_valores trimmed
                from _psi_oot;
            quit;
            %if &num_valores. <= 10 %then %let es_categorica = 1;
        %end;

        %put NOTE: [psi_compute] Variable &v_aux. (categorica=&es_categorica.);

        /* ---- PSI Mensual (por periodo) ---- */
        %if &mensual. = 1 and &n_meses. > 0 %then %do;
            %let m = 1;
            %let c = %scan(&meses_oot., &m., %str( ));

            %do %while(%length(&c.) > 0);

                data _psi_oot_mes;
                    set _psi_oot;
                    where &byvar. = &c.;
                run;

                %_psi_calc(
                    dev=_psi_dev,
                    oot=_psi_oot_mes,
                    var=&v_aux.,
                    n_buckets=&n_buckets.,
                    flg_continue=%eval(1 - &es_categorica.)
                );

                proc sql;
                    insert into _psi_cubo (Variable, Periodo, PSI, Tipo)
                    values ("&v_aux.", &c., &_psi_valor., "Mensual");
                quit;

                %let m = %eval(&m. + 1);
                %let c = %scan(&meses_oot., &m., %str( ));
            %end;
        %end;

        /* ---- PSI Total (DEV vs OOT completo) ---- */
        %_psi_calc(
            dev=_psi_dev,
            oot=_psi_oot,
            var=&v_aux.,
            n_buckets=&n_buckets.,
            flg_continue=%eval(1 - &es_categorica.)
        );

        proc sql;
            insert into _psi_cubo (Variable, Periodo, PSI, Tipo)
            values ("&v_aux.", 999999, &_psi_valor., "Total");
        quit;

        %let n = %eval(&n. + 1);
        %let v = %scan(&_combined_vars., &n., %str( ));
    %end;

    /* ==================================================================
       6) Cubo Wide (pivot Variable × Periodo)
       ================================================================== */
    proc sort data=_psi_cubo; by Variable Periodo; run;

    %let hay_mensual = 0;
    proc sql noprint;
        select count(*) into :hay_mensual trimmed
        from _psi_cubo where Tipo = "Mensual";
    quit;

    %if &hay_mensual. > 0 %then %do;
        proc transpose data=_psi_cubo(where=(Tipo="Mensual"))
            out=_psi_cubo_wide(drop=_NAME_)
            prefix=mes_;
            by Variable;
            id Periodo;
            var PSI;
        run;

        /* Agregar PSI_Total como columna */
        proc sql;
            create table _psi_wide_tmp as
            select a.*, b.PSI as PSI_Total format=10.6
            from _psi_cubo_wide a
            left join _psi_cubo(where=(Tipo="Total")) b
                on a.Variable = b.Variable;
        quit;

        proc datasets lib=work nolist nowarn;
            delete _psi_cubo_wide;
            change _psi_wide_tmp = _psi_cubo_wide;
        quit;
    %end;
    %else %do;
        /* Sin datos mensuales - wide solo con Total */
        proc sql;
            create table _psi_cubo_wide as
            select Variable, PSI as PSI_Total format=10.6
            from _psi_cubo
            where Tipo = "Total";
        quit;
    %end;

    /* ==================================================================
       7) Resumen con estadísticas y alertas
       ================================================================== */
    proc sql;
        create table _psi_resumen as
        select
            Variable,
            max(case when Tipo="Total" then PSI else . end)
                as PSI_Total format=10.6,
            min(case when Tipo="Mensual" then PSI else . end)
                as PSI_Min format=10.6,
            max(case when Tipo="Mensual" then PSI else . end)
                as PSI_Max format=10.6,
            mean(case when Tipo="Mensual" then PSI else . end)
                as PSI_Mean format=10.6,
            std(case when Tipo="Mensual" then PSI else . end)
                as PSI_Std format=10.6,
            sum(case when Tipo="Mensual" and PSI < 0.10 then 1 else 0 end)
                as Meses_Verde,
            sum(case when Tipo="Mensual" and PSI >= 0.10 and PSI < 0.25 then 1 else 0 end)
                as Meses_Amarillo,
            sum(case when Tipo="Mensual" and PSI >= 0.25 then 1 else 0 end)
                as Meses_Rojo,
            sum(case when Tipo="Mensual" then 1 else 0 end)
                as Total_Meses,
            max(case when Tipo="Mensual" then Periodo else . end)
                as Ultimo_Mes,
            min(case when Tipo="Mensual" then Periodo else . end)
                as Primer_Mes
        from _psi_cubo
        group by Variable;
    quit;

    /* Agregar tendencia y alertas */
    proc sql;
        create table _psi_rsmn_tmp as
        select
            a.*,
            b.PSI as PSI_Primer_Mes format=10.6,
            c.PSI as PSI_Ultimo_Mes format=10.6,
            coalesce(c.PSI, 0) - coalesce(b.PSI, 0) as Tendencia format=10.6,
            case
                when a.PSI_Total < 0.10  then 'VERDE'
                when a.PSI_Total < 0.25  then 'AMARILLO'
                else 'ROJO'
            end as Semaforo_Total length=10,
            case
                when coalesce(c.PSI, 0) - coalesce(b.PSI, 0) > 0.05  then 'EMPEORANDO'
                when coalesce(c.PSI, 0) - coalesce(b.PSI, 0) < -0.05 then 'MEJORANDO'
                else 'ESTABLE'
            end as Alerta_Tendencia length=15,
            case
                when a.Total_Meses > 0 then a.Meses_Rojo / a.Total_Meses
                else 0
            end as Pct_Meses_Rojo format=percent8.1
        from _psi_resumen a
        left join _psi_cubo b
            on a.Variable = b.Variable
            and a.Primer_Mes = b.Periodo
            and b.Tipo = "Mensual"
        left join _psi_cubo c
            on a.Variable = c.Variable
            and a.Ultimo_Mes = c.Periodo
            and c.Tipo = "Mensual";
    quit;

    proc datasets lib=work nolist nowarn;
        delete _psi_resumen;
        change _psi_rsmn_tmp = _psi_resumen;
    quit;

    /* ==================================================================
       8) Cleanup intermedios (outputs se preservan para report/persist)
       ================================================================== */
    proc datasets lib=work nolist nowarn;
        delete _psi_dev _psi_oot _psi_oot_mes;
    quit;

    %put NOTE: [psi_compute] Completado - cubo=_psi_cubo wide=_psi_cubo_wide resumen=_psi_resumen;

%mend _psi_compute;
