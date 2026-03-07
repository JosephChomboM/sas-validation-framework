/* =========================================================================
psi_compute.sas - Core de cómputo del PSI

Contiene dos macros:

1) %_psi_calc - PSI para UNA variable (core unitario)
Input:  dos datasets (dev, oot) y nombre de variable
Output: macro variable &psi_valor con el valor PSI

2) %_psi_compute - Orquestador: itera variables × periodos
Input:  tablas CAS promovidas (vía caslib.table), listas de vars,
variable temporal (byvar), parámetros de discretización
Output: tres tablas en casuser (CAS):
- casuser._psi_cubo       Variable × Periodo × PSI × Tipo
- casuser._psi_cubo_wide  pivot Variable × meses + PSI_Total
- casuser._psi_resumen    estadísticas + semáforo + alertas tendencia

Migrado de psi_legacy.sas (_psi_calc + __psi_variables).
Adaptado para tablas CAS (input_caslib.table en lugar de datasets locales).
========================================================================= */

/* =====================================================================
%_psi_calc - PSI para una variable
Método: PROC RANK (continuas) o valores directos (categóricas)
Suavizado Laplace: (n + 0.5) / (total + 0.5 * n_buckets)
===================================================================== */
%macro _psi_calc(dev=, oot=, var=, n_buckets=10, flg_continue=1);

    %local rnd n_cortes i;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    %global psi_valor;
    %let psi_valor=0;

    /*----- Variables continuas: crear buckets con PROC RANK -----*/
    %if &flg_continue.=1 %then %do;

        proc rank data=&dev.(keep=&var.) out=_rank_dev_&rnd. groups=&n_buckets.;
            var &var.;
            ranks bucket;
        run;

        proc sql noprint;
            create table _cortes_&rnd. as select bucket, max(&var.) as corte
                from _rank_dev_&rnd. where bucket is not missing group by bucket
                order by bucket;

            select count(*) into :n_cortes trimmed from _cortes_&rnd.;
        quit;

        %if &n_cortes. > 0 %then %do;
            proc sql noprint;
                select corte into :corte1 - :corte&n_cortes. from _cortes_&rnd.;
            quit;

            /* Asignar buckets a DEV usando los cortes */
            data _dev_bucket_&rnd.;
                set &dev.(keep=&var.);
                if missing(&var.) then bucket=0;
                %do i=1 %to &n_cortes.;
                    %if &i.=1 %then %do;
                        else if &var. <= &&corte&i. then bucket=&i.;
                    %end;
                    %else %if &i.=&n_cortes. %then %do;
                        else bucket=&i.;
                    %end;
                    %else %do;
                        else if &var. <= &&corte&i. then bucket=&i.;
                    %end;
                %end;
            run;

            /* Asignar buckets a OOT usando los mismos cortes */
            data _oot_bucket_&rnd.;
                set &oot.(keep=&var.);
                if missing(&var.) then bucket=0;
                %do i=1 %to &n_cortes.;
                    %if &i.=1 %then %do;
                        else if &var. <= &&corte&i. then bucket=&i.;
                    %end;
                    %else %if &i.=&n_cortes. %then %do;
                        else bucket=&i.;
                    %end;
                    %else %do;
                        else if &var. <= &&corte&i. then bucket=&i.;
                    %end;
                %end;
            run;
        %end;
        %else %do;
            data _dev_bucket_&rnd.;
                set &dev.(keep=&var.);
                bucket=1;
            run;

            data _oot_bucket_&rnd.;
                set &oot.(keep=&var.);
                bucket=1;
            run;
        %end;

    %end;
    /*----- Variables categóricas: usar valores como bucket -----*/
    %else %do;

        data _dev_bucket_&rnd.;
            set &dev.(keep=&var.);
            bucket=&var.;
        run;

        data _oot_bucket_&rnd.;
            set &oot.(keep=&var.);
            bucket=&var.;
        run;

    %end;

    /*----- Calcular frecuencias y PSI con SQL -----*/
    proc sql noprint;
        create table _freq_dev_&rnd. as select bucket, count(*) as n_dev from
            _dev_bucket_&rnd. group by bucket;

        create table _freq_oot_&rnd. as select bucket, count(*) as n_oot from
            _oot_bucket_&rnd. group by bucket;

        select coalesce(sum(n_dev), 0) into :tot_dev trimmed from
            _freq_dev_&rnd.;
        select coalesce(sum(n_oot), 0) into :tot_oot trimmed from
            _freq_oot_&rnd.;
    quit;

    %if &tot_dev. > 0 and &tot_oot. > 0 %then %do;
        proc sql noprint;
            create table _psi_calc_&rnd. as select coalesce(a.bucket, b.bucket)
                as bucket, coalesce(a.n_dev, 0) as n_dev, coalesce(b.n_oot, 0)
                as n_oot, (coalesce(a.n_dev, 0) + 0.5) / (&tot_dev. + 0.5 *
                &n_buckets.) as pct_dev, (coalesce(b.n_oot, 0) + 0.5) /
                (&tot_oot. + 0.5 * &n_buckets.) as pct_oot from _freq_dev_&rnd.
                a full join _freq_oot_&rnd. b on a.bucket=b.bucket;

            select sum((pct_oot - pct_dev) * log(pct_oot / pct_dev)) into
                :psi_valor trimmed from _psi_calc_&rnd.;
        quit;
    %end;

    %if %length(&psi_valor.)=0 or &psi_valor.=. %then %let psi_valor=0;

    /* Cleanup temporales */
    proc datasets lib=casuser nolist nowarn;
        delete _rank_dev_&rnd. _cortes_&rnd. _dev_bucket_&rnd. _oot_bucket_&rnd.
            _freq_dev_&rnd. _freq_oot_&rnd. _psi_calc_&rnd.;
    quit;

%mend _psi_calc;

/* =====================================================================
%_psi_compute - Orquestador: variables × periodos → cubo + wide + resumen
Genera tablas en casuser (CAS):
casuser._psi_cubo        detalle Variable × Periodo × PSI × Tipo
casuser._psi_cubo_wide   pivot Variable × meses + PSI_Total
casuser._psi_resumen     estadísticas + semáforo + alertas tendencia

Parámetros:
input_caslib  = CASLIB de las tablas promovidas
train_table   = nombre tabla TRAIN promovida
oot_table     = nombre tabla OOT promovida
vars_num      = lista de variables numéricas (separadas por espacio)
vars_cat      = lista de variables categóricas (separadas por espacio)
byvar         = variable temporal para breakdown mensual
n_buckets     = número de bins para PROC RANK (default 10)
mensual       = 1=breakdown mensual, 0=solo total
===================================================================== */
%macro _psi_compute( input_caslib=, train_table=, oot_table=, vars_num=,
    vars_cat=, byvar=, n_buckets=10, mensual=1 );

    %local n v m c z v_aux v_cat es_categorica num_valores lista_var meses_oot
        n_meses hay_mensual;

    %put NOTE: [psi_compute] Iniciando cómputo PSI...;
    %put NOTE: [psi_compute] vars_num=&vars_num.;
    %put NOTE: [psi_compute] vars_cat=&vars_cat.;
    %put NOTE: [psi_compute] byvar=&byvar. mensual=&mensual.;

    /* ---- 1) Construir lista unificada (categóricas marcadas con #) ----- */
    %let lista_var=&vars_num.;

    %let z=1;
    %let v_cat=%scan(&vars_cat., &z., %str( ));

    %do %while(%length(&v_cat.) > 0);
        %let lista_var=&lista_var. &v_cat.#;
        %let z=%eval(&z. + 1);
        %let v_cat=%scan(&vars_cat., &z., %str( ));
    %end;

    /* ---- 2) Copiar tablas CAS a WORK para procesamiento local ---------- */
    data casuser._psi_dev;
        set &input_caslib..&train_table.;
    run;

    data casuser._psi_oot;
        set &input_caslib..&oot_table.;
    run;

    /* ---- 3) Obtener lista de meses en OOT ------------------------------ */
    %let meses_oot= ;
    %let n_meses=0;

    %if %length(%superq(byvar)) > 0 %then %do;
        proc sql noprint;
            select distinct &byvar. into :meses_oot separated by ' ' from
                casuser._psi_oot order by &byvar.;

            select count(distinct &byvar.) into :n_meses trimmed from
                casuser._psi_oot;
        quit;
    %end;

    %put NOTE: [psi_compute] Meses OOT: &meses_oot. (n=&n_meses.);

    /* ---- 4) Inicializar CUBO PSI --------------------------------------- */
    %if %length(%superq(byvar)) > 0 %then %do;
        proc sql;
            create table casuser._psi_cubo ( Variable char(64), &byvar. num, PSI
                num format=10.6, Tipo char(15) );
        quit;
    %end;
    %else %do;
        proc sql;
            create table casuser._psi_cubo ( Variable char(64), Periodo num, PSI
                num format=10.6, Tipo char(15) );
        quit;
    %end;

    /* ---- 5) Calcular PSI por variable y mes ---------------------------- */
    %let n=1;
    %let v=%scan(&lista_var., &n., %str( ));

    %do %while(%length(&v.) > 0);

        /* Determinar si es categórica */
        %let es_categorica=0;
        %let v_aux=&v.;

        %if %index(&v., %str(#)) > 0 %then %do;
            %let v_aux=%substr(&v., 1, %eval(%length(&v.) - 1));
            %let es_categorica=1;
        %end;
        %else %do;
            proc sql noprint;
                select count(distinct &v_aux.) into :num_valores trimmed from
                    casuser._psi_oot;
            quit;
            %if &num_valores. <= 10 %then %let es_categorica=1;
        %end;

        %put NOTE: [psi_compute] Variable &v_aux. (categorica=&es_categorica.);

        /*----- PSI Mensual -----*/
        %if &mensual.=1 and &n_meses. > 0 %then %do;
            %let m=1;
            %let c=%scan(&meses_oot., &m., %str( ));

            %do %while(%length(&c.) > 0);

                data casuser._psi_oot_mes;
                    set casuser._psi_oot;
                    where &byvar.=&c.;
                run;

                %_psi_calc( dev=casuser._psi_dev, oot=casuser._psi_oot_mes,
                    var=&v_aux., n_buckets=&n_buckets., flg_continue=%eval(1 -
                    &es_categorica.) );

                proc sql;
                    insert into casuser._psi_cubo (Variable, &byvar., PSI, Tipo)
                        values ("&v_aux.", &c., &psi_valor., "Mensual");
                quit;

                %let m=%eval(&m. + 1);
                %let c=%scan(&meses_oot., &m., %str( ));
            %end;
        %end;

        /*----- PSI Total (DEV vs OOT completo) -----*/
        %_psi_calc( dev=casuser._psi_dev, oot=casuser._psi_oot, var=&v_aux.,
            n_buckets=&n_buckets., flg_continue=%eval(1 - &es_categorica.) );

        %if %length(%superq(byvar)) > 0 %then %do;
            proc sql;
                insert into casuser._psi_cubo (Variable, &byvar., PSI, Tipo) values
                    ("&v_aux.", 999999, &psi_valor., "Total");
            quit;
        %end;
        %else %do;
            proc sql;
                insert into casuser._psi_cubo (Variable, Periodo, PSI, Tipo) values
                    ("&v_aux.", 999999, &psi_valor., "Total");
            quit;
        %end;

        %let n=%eval(&n. + 1);
        %let v=%scan(&lista_var., &n., %str( ));
    %end;

    /* ---- 6) Crear CUBO formato wide (Variable × Mes) ------------------- */
    %if %length(%superq(byvar)) > 0 %then %do;

        proc sort data=casuser._psi_cubo;
            by Variable &byvar.;
        run;

        %let hay_mensual=0;

        proc sql noprint;
            select count(*) into :hay_mensual trimmed from casuser._psi_cubo where
                Tipo="Mensual";
        quit;

        %if &hay_mensual. > 0 %then %do;
            proc transpose data=casuser._psi_cubo(where=(Tipo="Mensual"))
                out=casuser._psi_cubo_wide(drop=_NAME_) prefix=mes_;
                by Variable;
                id &byvar.;
                var PSI;
            run;

            proc sql;
                create table casuser._psi_wide_tmp as select a.*, b.PSI as
                    PSI_Total format=10.6 from casuser._psi_cubo_wide a left join
                    casuser._psi_cubo(where=(Tipo="Total")) b on a.Variable=
                    b.Variable;
            quit;

            proc datasets lib=casuser nolist nowarn;
                delete _psi_cubo_wide;
                change _psi_wide_tmp=_psi_cubo_wide;
            quit;
        %end;
        %else %do;
            proc sql;
                create table casuser._psi_cubo_wide as select Variable, PSI as
                    PSI_Total format=10.6 from casuser._psi_cubo where
                    Tipo="Total";
            quit;
        %end;

        /* ---- 7) Resumen con estadísticas y alertas --------------------- */
        proc sql;
            create table casuser._psi_resumen as select Variable, max(case when
                Tipo="Total" then PSI else . end) as PSI_Total format=10.6,
                min(case when Tipo="Mensual" then PSI else . end) as PSI_Min
                format=10.6, max(case when Tipo="Mensual" then PSI else . end)
                as PSI_Max format=10.6, mean(case when Tipo="Mensual" then PSI
                else . end) as PSI_Mean format=10.6, std(case when
                Tipo="Mensual" then PSI else . end) as PSI_Std format=10.6,
                sum(case when Tipo="Mensual" and PSI < 0.10 then 1 else 0 end)
                as Meses_Verde, sum(case when Tipo="Mensual" and PSI >= 0.10 and
                PSI < 0.25 then 1 else 0 end) as Meses_Amarillo, sum(case when
                Tipo="Mensual" and PSI >= 0.25 then 1 else 0 end) as Meses_Rojo,
                sum(case when Tipo="Mensual" then 1 else 0 end) as Total_Meses,
                max(case when Tipo="Mensual" then &byvar. else . end) as
                Ultimo_Mes, min(case when Tipo="Mensual" then &byvar. else .
                end) as Primer_Mes from casuser._psi_cubo group by Variable;
        quit;

        /* Agregar tendencia y alertas */
        proc sql;
            create table casuser._psi_resumen_tmp as select a.*, b.PSI as
                PSI_Primer_Mes format=10.6, c.PSI as PSI_Ultimo_Mes format=10.6,
                coalesce(c.PSI, 0) - coalesce(b.PSI, 0) as Tendencia
                format=10.6, case when a.PSI_Total < 0.10 then 'VERDE' when
                a.PSI_Total < 0.25 then 'AMARILLO' else 'ROJO' end as
                Semaforo_Total length=10, case when coalesce(c.PSI, 0) -
                coalesce(b.PSI, 0) > 0.05 then 'EMPEORANDO' when coalesce(c.PSI,
                0) - coalesce(b.PSI, 0) < -0.05 then 'MEJORANDO' else 'ESTABLE'
                end as Alerta_Tendencia length=15, case when a.Total_Meses > 0
                then a.Meses_Rojo / a.Total_Meses else 0 end as Pct_Meses_Rojo
                format=percent8.1 from casuser._psi_resumen a left join
                casuser._psi_cubo b on a.Variable=b.Variable and a.Primer_Mes=
                b.&byvar. and b.Tipo="Mensual" left join casuser._psi_cubo c on
                a.Variable=c.Variable and a.Ultimo_Mes=c.&byvar. and
                c.Tipo="Mensual";
        quit;

        proc datasets lib=casuser nolist nowarn;
            delete _psi_resumen;
            change _psi_resumen_tmp=_psi_resumen;
        quit;

    %end;
    %else %do;
        /* Sin byvar: solo cubo wide con Total y resumen simplificado */
        proc sql;
            create table casuser._psi_cubo_wide as select Variable, PSI as
                PSI_Total format=10.6 from casuser._psi_cubo where Tipo="Total";
        quit;

        proc sql;
            create table casuser._psi_resumen as select Variable, PSI as PSI_Total
                format=10.6, case when PSI < 0.10 then 'VERDE' when PSI < 0.25
                then 'AMARILLO' else 'ROJO' end as Semaforo_Total length=10 from
                casuser._psi_cubo where Tipo="Total";
        quit;
    %end;

    /* ---- 8) Cleanup temporales ----------------------------------------- */
    proc datasets lib=casuser nolist nowarn;
        delete _psi_dev _psi_oot _psi_oot_mes;
    quit;

    %put NOTE: [psi_compute] Cómputo completado. Tablas generadas:;
    %put NOTE: [psi_compute] casuser._psi_cubo;
    %put NOTE: [psi_compute] casuser._psi_cubo_wide;
    %put NOTE: [psi_compute] casuser._psi_resumen;

%mend _psi_compute;
