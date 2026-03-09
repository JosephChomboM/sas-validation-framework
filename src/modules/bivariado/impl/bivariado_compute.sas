/* =========================================================================
bivariado_compute.sas - Computo de analisis bivariado (tendencia)

Contiene macros que discretizan variables numericas via PROC RANK,
calculan tasa de default (RD) por bucket, y generan graficos de
tendencia. Para categoricas, agrupa directamente sin discretizar.

Macros:
%_biv_calcular_cortes  - Calcula puntos de corte via PROC RANK
%_biv_tendencia        - Aplica cortes/agrupcion y genera report+grafico
%_biv_trend_variables  - Orquestador: itera vars num+cat, TRAIN luego OOT

Patron de cortes (TRAIN → OOT):
- Para numericas, los cortes se calculan con TRAIN (reuse_cuts=0).
- Para OOT, se reutilizan cortes de TRAIN (reuse_cuts=1, tabla work.cortes).
- Para categoricas, no se usan cortes (flg_continue=0).

Tablas temporales en work (no CAS — requiere PROC RANK, DATA step
dinámico con macro injection &DATAAPPLY).
========================================================================= */

/* =====================================================================
%_biv_calcular_cortes - Calcula cortes via PROC RANK + etiquetas
Crea tabla work.cortes con: variable, rango, inicio, fin, etiqueta
===================================================================== */
%macro _biv_calcular_cortes(tablain, var, groups);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    /* Copiar datos a work para PROC RANK */
    data work._biv_rk_&rnd._1;
        set &tablain.(keep=&var.);
        &var. = round(&var., 0.0001);
    run;

    proc rank data=work._biv_rk_&rnd._1 out=work._biv_rk_&rnd._2
        groups=&groups.;
        ranks RANGO;
        var &var.;
    run;

    proc sql noprint;
        create table work._biv_rk_&rnd._3 as select RANGO, min(&var.) as MINVAL,
            max(&var.) as MAXVAL from work._biv_rk_&rnd._2 group by RANGO;
    quit;

    proc sort data=work._biv_rk_&rnd._3;
        by RANGO;
    run;

    data work._biv_rk_&rnd._4;
        set work._biv_rk_&rnd._3(rename=(RANGO=RANGO_INI)) end=EOF;
        retain MARCA 0;
        N=_n_;
        FLAG_INI=0;
        FLAG_FIN=0;
        LAGMAXVAL=lag(MAXVAL);
        RANGO=RANGO_INI + 1;
        if RANGO_INI=. then RANGO=0;
        if RANGO_INI >= 0 then MARCA=MARCA + 1;
        if MARCA=1 then FLAG_INI=1;
        if EOF then FLAG_FIN=1;
    run;

    proc sql noprint;
        create table work.cortes as select "&var." as VARIABLE length=32, RANGO,
            RANGO_INI, LAGMAXVAL as INICIO, MAXVAL as FIN, FLAG_INI, FLAG_FIN,
            case when RANGO=0 then "00. Missing" when FLAG_INI=1 then
            cat(put(RANGO, Z2.), ". <-Inf; ", cats(put(MAXVAL, F12.4)), "]")
            when FLAG_FIN=1 then cat(put(RANGO, Z2.), ". <", cats(put(LAGMAXVAL,
            F12.4)), "; +Inf>") else cat(put(RANGO, Z2.), ". <",
            cats(put(LAGMAXVAL, F12.4)), "; ", cats(put(MAXVAL, F12.4)), "]")
            end as ETIQUETA length=200 from work._biv_rk_&rnd._4;
    quit;

    /* Cleanup staging */
    proc datasets library=work nolist nowarn;
        delete _biv_rk_&rnd.:;
    quit;

%mend _biv_calcular_cortes;

/* =====================================================================
%_biv_tendencia - Aplica cortes a datos, calcula N/pct/defaults/RD
Para numericas (flg_continue=1): usa cortes de work.cortes.
Para categoricas (flg_continue=0): agrupa directamente.
Genera tabla + grafico de barras (pct) + linea (RD).
===================================================================== */
%macro _biv_tendencia(tablain, var, target=, groups=5, flg_continue=1,
    reuse_cuts=0, m_data_type=);

    %local rnd _total DATAAPPLY;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    /* Copiar datos a work, reemplazar dummies con missing */
    data work._biv_t_&rnd._0;
        set &tablain.;
        %if &flg_continue.=1 %then %do;
            if &var. in (., 1111111111, -1111111111, 2222222222, -2222222222,
                3333333333, -3333333333, 4444444444, 5555555555, 6666666666,
                7777777777, -999999999) then &var.=.;
        %end;
    run;

    /* Obtener total de obs */
    data _null_;
        if 0 then set work._biv_t_&rnd._0 nobs=n;
        call symputx("_total", n);
        stop;
    run;

    %if &flg_continue.=1 %then %do;
        /* --- Numerica: discretizar --- */
        /* Calcular cortes si no reusamos */
        %if &reuse_cuts.=0 %then %do;
            %_biv_calcular_cortes(work._biv_t_&rnd._0, &var., &groups.);

            proc sort data=work.cortes;
                by rango;
            run;
        %end;
        /* Si reuse_cuts=1, work.cortes ya existe de TRAIN */

        /* Construir sentencias CASE para etiquetar */
        data work._biv_t_&rnd._1;
            set work.cortes end=EOF;
            length QUERY_BODY $500;
            if RANGO=0 then QUERY_BODY=cat("IF MISSING(&var.)=1 THEN ETIQUETA=",
                '"', strip(ETIQUETA), '";');
            else if FLAG_INI=1 then QUERY_BODY=cat("IF &var.<=", FIN,
                " AND &var.>. THEN ETIQUETA=", '"', strip(ETIQUETA), '";');
            else if FLAG_FIN=1 then QUERY_BODY=cat("IF &var.>", INICIO,
                " THEN ETIQUETA=", '"', strip(ETIQUETA), '";');
            else QUERY_BODY=cat("IF ", INICIO, "<&var.<=", FIN,
                " THEN ETIQUETA=", '"', strip(ETIQUETA), '";');
        run;

        proc sql noprint;
            select QUERY_BODY into :DATAAPPLY separated by " " from
                work._biv_t_&rnd._1;
        quit;

        data work._biv_t_&rnd._2;
            set work._biv_t_&rnd._0;
            length ETIQUETA $ 200;
            &DATAAPPLY.;
        run;

        proc sql noprint;
            create table work._biv_report as select ETIQUETA as &var., count(*)
                as n, count(*) / &_total. as pct_cuentas format=percent8.0,
                sum(&target.) as defaults, mean(&target.) as RD
                format=percent8.2 from work._biv_t_&rnd._2 group by ETIQUETA;
        quit;
    %end;
    %else %do;
        /* --- Categorica: agrupar directamente --- */
        proc sql noprint;
            create table work._biv_report as select &var., count(*) as n,
                count(*) / &_total. as pct_cuentas format=percent8.0,
                sum(&target.) as defaults, mean(&target.) as RD
                format=percent8.2 from work._biv_t_&rnd._0 group by &var.;
        quit;
    %end;

    title "Tendencia &var. - &m_data_type.";

    proc sgplot data=work._biv_report subpixel noautolegend;
        yaxis label="% Cuentas (bar)" discreteorder=data;
        y2axis min=0 label="RD";
        vbar &var. / response=pct_cuentas nooutline barwidth=0.4;
        vline &var. / response=rd markers markerattrs=(symbol=circlefilled)
            y2axis;
        xaxis label="Buckets variable" valueattrs=(size=8pt);
    run;
    title;

    proc print data=work._biv_report noobs;
    run;

    /* Cleanup staging */
    proc datasets library=work nolist nowarn;
        delete _biv_t_&rnd.: _biv_report;
    quit;

%mend _biv_tendencia;

/* =====================================================================
%_biv_trend_variables - Orquestador: itera vars num+cat
Para cada variable numerica: TRAIN (calcula cortes) + OOT (reusa cortes).
Para cada variable categorica: TRAIN + OOT (sin cortes).
===================================================================== */
%macro _biv_trend_variables(train_data=, oot_data=, target=, vars_num=,
    vars_cat=, groups=5);

    %local c v z v_cat;

    /* Procesar variables numericas */
    %if %length(&vars_num.) > 0 %then %do;
        %let c=1;
        %let v=%scan(&vars_num., &c., %str( ));
        %do %while(%length(&v.) > 0);
            %put NOTE: [bivariado] Procesando variable numerica: &v.;

            /* TRAIN: calcular cortes */
            %_biv_tendencia(&train_data., &v., target=&target., groups=&groups.,
                flg_continue=1, reuse_cuts=0, m_data_type=TRAIN);

            /* OOT: reusar cortes de TRAIN */
            %_biv_tendencia(&oot_data., &v., target=&target., groups=&groups.,
                flg_continue=1, reuse_cuts=1, m_data_type=OOT);

            /* Limpiar cortes despues de OOT */
            proc datasets library=work nolist nowarn;
                delete cortes;
            quit;

            %let c=%eval(&c. + 1);
            %let v=%scan(&vars_num., &c., %str( ));
        %end;
    %end;

    /* Procesar variables categoricas */
    %if %length(&vars_cat.) > 0 %then %do;
        %let z=1;
        %let v_cat=%scan(&vars_cat., &z., %str( ));
        %do %while(%length(&v_cat.) > 0);
            %put NOTE: [bivariado] Procesando variable categorica: &v_cat.;

            /* TRAIN */
            %_biv_tendencia(&train_data., &v_cat., target=&target.,
                groups=&groups., flg_continue=0, reuse_cuts=0,
                m_data_type=TRAIN);

            /* OOT */
            %_biv_tendencia(&oot_data., &v_cat., target=&target.,
                groups=&groups., flg_continue=0, reuse_cuts=0, m_data_type=OOT);

            %let z=%eval(&z. + 1);
            %let v_cat=%scan(&vars_cat., &z., %str( ));
        %end;
    %end;

%mend _biv_trend_variables;
