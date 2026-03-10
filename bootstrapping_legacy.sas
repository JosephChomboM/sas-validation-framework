/*--------------------------------------------------------------
  Version: 2.1
  Desarrollador: Joseph Chombo
  Fecha Release: 01/12/2025
  Módulo: Bootstrapping - Análisis de estabilidad de coeficientes
  Objetivo: Detectar inestabilidad de signos en betas para PD scoring
--------------------------------------------------------------*/

%macro __boots(
    tablain,
    tablaoot,
    nrounds,
    lista_variables,
    target,
    samprate=1,
    seed=12345,
    ponderada=0,
    hits_dev=1,
    hits_oot=1,
    where_oot=.
);

    %local rnd i;
    %let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));

    /*=========================================
      1. Preparación de TRAIN y OOT
      =========================================*/

    /* TRAIN */
    %if &hits_dev=1 %then %do;
        data t_&rnd._t1;
            set &tablain.;
            *numhits = 1;
        run;
    %end;
    %else %do;
        data t_&rnd._t1;
            set &tablain.;
            *numhits = &hits_dev;
        run;
    %end;

    /* OOT */
    %if &hits_oot=1 %then %do;
        data t_&rnd._t2;
            set &tablaoot.;
            *numhits = 1;
            *%if &where_oot ne . %then %do;
                *where &byvar <= &where_oot;
            *%end;
        run;
    %end;
    %else %do;
        data t_&rnd._t2;
            set &tablaoot.;
            *numhits = &hits_oot;
            *%if &where_oot ne . %then %do;
            *    where &byvar <= &where_oot;
            *%end;
        run;
    %end;

    proc sort data=t_&rnd._t1;
        by &target.;
    run;

    /*=========================================
      2. Bootstrap
      =========================================*/

    %if &ponderada = 0 %then %do;

        ods select none;
        proc surveyselect data=t_&rnd._t1
            out=t_&rnd._boots_dev
            seed=&seed.
            method=urs
            samprate=&samprate.
            nmin=1
            outhits
            reps=&nrounds.;
            strata &target.;
        run;

        proc sort data=t_&rnd._boots_dev; by Replicate;run;

        proc logistic data=t_&rnd._boots_dev namelen=45;
            by replicate;
            model &target.(event="1") = &lista_variables.;
            ods output parameterestimates=t_&rnd._boots_betas;
        run;
        ods select all;

        data tablaout;
            set t_&rnd._boots_betas;
            where Variable ^= "Intercept";
            iteracion = replicate;
            keep Variable Estimate StdErr ProbChiSq iteracion;
        run;

    %end;
    %else %do;
        ods select none;
        %do i=1 %to &nrounds.;

            proc surveyselect data=t_&rnd._t1 noprint out=t_&rnd._t3   
                seed = &i.
                method = urs 
                samprate = &samprate.
                outhits;
                strata &target.;
            run;
            
            %_reg_log_ponderada(t_&rnd._t3, &lista_variables., &target., numhits);

            %if &i=1 %then %do;
                proc sql;
                    create table tablaout as
                    select Variable, Estimate, StdErr, ProbChiSq, &i as iteracion
                    from t_&rnd._t3_betas
                    where Variable ^= "Intercept";
                quit;
            %end;
            %else %do;
                proc sql;
                    insert into tablaout
                    select Variable, Estimate, StdErr, ProbChiSq, &i as iteracion
                    from t_&rnd._t3_betas
                    where Variable ^= "Intercept";
                quit;
            %end;

        %end;
        ods select all;

    %end;

    /*=========================================
      3. Modelos finales TRAIN/OOT + pesos
      =========================================*/

    %if &ponderada. %then %do;

        %_reg_log_ponderada(t_&rnd._t1, &lista_variables., &target., numhits);
        %_pesos(t_&rnd._t1, t_&rnd._t1_betas, &target., &lista_variables.);

        proc sql;
            create table betas1 as
            select a.Variable, a.Estimate, a.StdErr, a.ProbChiSq, b.peso format=8.4
            from t_&rnd._t1_betas a
            left join pesos_report b on upcase(a.Variable) = upcase(b.Variable);
        quit;

        %_reg_log_ponderada(t_&rnd._t2, &lista_variables., &target., numhits);
        %_pesos(t_&rnd._t2, t_&rnd._t2_betas, &target., &lista_variables.);

        proc sql;
            create table betas2 as
            select a.Variable, a.Estimate, a.StdErr, a.ProbChiSq, b.peso format=8.4
            from t_&rnd._t2_betas a
            left join pesos_report b on upcase(a.Variable) = upcase(b.Variable);
        quit;

    %end;
    %else %do;

        %_regresion(t_&rnd._t1, &target., &lista_variables.);
        %_pesos(t_&rnd._t1, t_&rnd._t1_betas, &target., &lista_variables.);

        proc sql;
            create table betas1 as
            select a.Variable, a.Estimate, a.StdErr, a.ProbChiSq, b.peso format=8.4
            from t_&rnd._t1_betas a
            left join pesos_report b on upcase(a.Variable) = upcase(b.Variable);
        quit;

        %_regresion(t_&rnd._t2, &target., &lista_variables.);
        %_pesos(t_&rnd._t2, t_&rnd._t2_betas, &target., &lista_variables.);

        proc sql;
            create table betas2 as
            select a.Variable, a.Estimate, a.StdErr, a.ProbChiSq, b.peso format=8.4
            from t_&rnd._t2_betas a
            left join pesos_report b on upcase(a.Variable) = upcase(b.Variable);
        quit;

    %end;

    /*=========================================
      4. CUBO: Tablón completo de iteraciones
         Formato wide: cada columna es una iteración
      =========================================*/

    proc sort data=tablaout;
        by Variable iteracion;
    run;

    /* Cubo formato wide: Variable | iter_1 | iter_2 | ... | iter_N */
    proc transpose data=tablaout out=cubo_wide(drop=_NAME_) prefix=iter_;
        by Variable;
        id iteracion;
        var Estimate;
    run;

    /*=========================================
      5. Estadísticas de estabilidad de signo
      =========================================*/

    /* Calcular métricas de estabilidad por variable */
    proc sql;
        create table bootstrap_stats as
        select 
            Variable,
            count(*) as n_iter,
            mean(Estimate) as beta_mean format=12.6,
            std(Estimate) as beta_std format=12.6,
            min(Estimate) as beta_min format=12.6,
            max(Estimate) as beta_max format=12.6,
            /* Percentiles clave */
            calculated beta_mean - 1.96*calculated beta_std as ci_lower_95 format=12.6,
            calculated beta_mean + 1.96*calculated beta_std as ci_upper_95 format=12.6,
            /* Análisis de cambio de signo */
            sum(case when Estimate > 0 then 1 else 0 end) as n_positivos,
            sum(case when Estimate < 0 then 1 else 0 end) as n_negativos,
            sum(case when Estimate = 0 then 1 else 0 end) as n_ceros,
            /* % de consistencia de signo */
            max(
                sum(case when Estimate > 0 then 1 else 0 end),
                sum(case when Estimate < 0 then 1 else 0 end)
            ) / count(*) as pct_signo_consistente format=percent8.1,
            /* Flag de inestabilidad */
            case 
                when sum(case when Estimate > 0 then 1 else 0 end) > 0 
                 and sum(case when Estimate < 0 then 1 else 0 end) > 0 
                then 'INESTABLE'
                else 'ESTABLE'
            end as flag_signo length=10
        from tablaout
        group by Variable;
    quit;

    /* Agregar betas del modelo final TRAIN y OOT */
    proc sql;
        create table report as
        select 
            a.*,
            b.Estimate as beta_dev format=12.6,
            b.StdErr as stderr_dev format=12.6,
            b.ProbChiSq as pval_dev format=8.4,
            b.peso as peso_dev format=8.4,
            c.Estimate as beta_oot format=12.6,
            c.StdErr as stderr_oot format=12.6,
            c.ProbChiSq as pval_oot format=8.4,
            c.peso as peso_oot format=8.4,
            /* Comparación TRAIN vs Bootstrap */
            case 
                when b.Estimate > 0 and a.n_negativos > 0 then 'TRAIN+ pero Bootstrap tiene negativos'
                when b.Estimate < 0 and a.n_positivos > 0 then 'TRAIN- pero Bootstrap tiene positivos'
                else 'Consistente'
            end as alerta_signo length=50
        from bootstrap_stats a
        left join betas1 b on upcase(a.Variable) = upcase(b.Variable)
        left join betas2 c on upcase(a.Variable) = upcase(c.Variable);
    quit;

    /* Agregar percentiles exactos */
    proc means data=tablaout noprint;
        by Variable;
        var Estimate;
        output out=percentiles(drop=_TYPE_ _FREQ_)
            p1=p1 p5=p5 p10=p10 p25=p25 p50=p50 p75=p75 p90=p90 p95=p95 p99=p99;
    run;

    proc sql;
        create table report_final as
        select a.*, 
               b.p1, b.p5, b.p10, b.p25, b.p50, b.p75, b.p90, b.p95, b.p99
        from report a
        left join percentiles b on a.Variable = b.Variable
        order by a.peso_dev desc;
    quit;

    /* Limpiar temporales */
    proc datasets nolist;
        delete t_&rnd.: betas1 betas2 bootstrap_stats percentiles report;
    run;

%mend;


/*--------------------------------------------------------------
  Macro para generar gráficos por variable
  Histograma + densidad con líneas de referencia (beta TRAIN y OOT)
--------------------------------------------------------------*/
%macro __plot_boots_by_var(data_iter=tablaout, data_report=report_final);

    %local dsid nobs i var_name beta_dev beta_oot;

    /* Obtener lista de variables */
    proc sql noprint;
        select distinct Variable into :var_list separated by '|'
        from &data_iter.;
        select count(distinct Variable) into :n_vars trimmed
        from &data_iter.;
    quit;

    %do i = 1 %to &n_vars.;
        %let var_name = %scan(&var_list., &i., |);

        /* Obtener beta TRAIN y OOT para esta variable */
        proc sql noprint;
            select beta_dev, beta_oot, flag_signo, pct_signo_consistente
            into :beta_dev trimmed, :beta_oot trimmed, :flag_signo trimmed, :pct_consist trimmed
            from &data_report.
            where Variable = "&var_name.";
        quit;

        /* Gráfico: Histograma con densidad y líneas de referencia */
        title "Bootstrap - &var_name.";
        title2 "Estabilidad: &flag_signo. (&pct_consist. consistente)";
        
        proc sgplot data=&data_iter.(where=(Variable="&var_name."));
            histogram Estimate / binwidth=0.01 transparency=0.3 fillattrs=(color=steelblue);
            density Estimate / type=kernel lineattrs=(color=navy thickness=2);
            refline 0 / axis=x lineattrs=(color=gray pattern=dash thickness=1) 
                        label="Cero" labelattrs=(size=8);
            refline &beta_dev. / axis=x lineattrs=(color=blue thickness=2) 
                        label="Beta TRAIN" labelattrs=(size=8 color=blue);
            refline &beta_oot. / axis=x lineattrs=(color=red thickness=2 pattern=shortdash) 
                        label="Beta OOT" labelattrs=(size=8 color=red);
            xaxis label="Valor del Coeficiente (Beta)" labelattrs=(size=10);
            yaxis label="Frecuencia" labelattrs=(size=10);
        run;
        
        title;
    %end;

%mend;


/*--------------------------------------------------------------
  Macro legacy para compatibilidad (gráfico resumen)
--------------------------------------------------------------*/
%macro __plot_boots(alias=);

    title "Bootstrapping modelo &alias.";

    proc sgplot data=report_final noautolegend;
        highlow x=Variable high=p95 low=p5 / type=bar 
            fillattrs=(color=LIGHTSTEELBLUE) NOOUTLINE;
        scatter x=Variable y=beta_dev / 
            markerattrs=(color=BLUE symbol=CircleFilled size=10)
            legendlabel="Beta TRAIN";
        scatter x=Variable y=beta_oot / 
            markerattrs=(color=RED symbol=DiamondFilled size=10)
            legendlabel="Beta OOT";
        refline 0 / axis=y lineattrs=(color=gray pattern=dash);
        *yaxis label="Betas (IC 90%: p5-p95)" labelattrs=(size=10);
        xaxis valueattrs=(size=8) labelattrs=(size=10) discreteorder=data;
    run;

    title;

%mend;


/*--------------------------------------------------------------
  Macros auxiliares (sin cambios)
--------------------------------------------------------------*/
%macro _reg_log_ponderada(tablain, variables, def, weight, get_intercept=0);

    %local rnd;
    %let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));

    ods select none;
    proc freq data=&tablain.;
        table &def. / missing out=t_&rnd._1;
        weight &weight.;
    run;

    data _null_;
        set t_&rnd._1; 
        where &def.=1;
        multiplier=(100-percent)/percent;
        call symput("multiplier",left(trim(put(multiplier,20.10))));
    run;

    data t_&rnd._2;
        set &tablain.;
        if &def.=1 then sample_w=&weight.*&multiplier.;
        else sample_w=&weight.;
    run;

    proc logistic data=t_&rnd._2 namelen=45;
        model &def.(event="1")= &variables.;
        weight sample_w;
        output out=&tablain._out pred=y_est;
        ods output NObs=&tablain._nobs;
        ods output ResponseProfile=&tablain._ResponseProfile;
        ods output FitStatistics=&tablain._FitStatistics;
        ods output parameterestimates=&tablain._betas;
        ods output association=&tablain._stats;
    run;  
    ods select all;

    %if &get_intercept = 0 %then %do;
        data &tablain._betas;
            set &tablain._betas;
            where Variable ^= "Intercept";
        run;
    %end;

    proc datasets nolist;
       delete t_&rnd.:;
    run;
%mend;


%macro _pesos(data, betas, target, lista_variables);

    %local rnd;
    %let rnd = %sysfunc(int(%sysfunc(ranuni(0))*100000));

    proc sort data=&data. out=t_&rnd._1; by &target.; run;

    ods select none;
    proc means data=t_&rnd._1 stackods; 
        var &lista_variables.;
        class &target.;
        ods output summary=t_&rnd._2;
    run;

    proc sort data=t_&rnd._2; by Variable; run;
    proc transpose data=t_&rnd._2 prefix=def out=t_&rnd._3(drop=_NAME_); 
        by Variable;
        id &target.;
        var Mean;
    run;
    ods select all;

    data t_&rnd._4;
        set t_&rnd._3;
        diff=abs(def1-def0);
    run;

    proc sql;
        create table t_&rnd._5 as
        select a.Variable, a.Estimate, b.diff
        from &betas a
        left join t_&rnd._4 b on a.Variable=b.Variable
        where a.Variable not in ("Intercept");
    quit;

    data pesos_report;
        set t_&rnd._5;
        pond = abs(Estimate*diff);
    run;

    proc sql noprint; 
        select sum(pond) into :sum_pond from pesos_report;
    quit;

    data pesos_report;
        set pesos_report;
        peso = pond/&sum_pond.;
    run;

    proc datasets nolist;
       delete t_&rnd.:;
    run;
%mend;


%macro _regresion(tablain, target, lista_variables, get_intercept=0);

    ods select none;
    proc logistic data=&tablain. namelen=45;
        model &target.(event="1")= &lista_variables.;
        output out=&tablain._out pred=y_est;
        ods output NObs=&tablain._nobs;
        ods output ResponseProfile=&tablain._ResponseProfile;
        ods output FitStatistics=&tablain._FitStatistics;
        ods output parameterestimates=&tablain._betas;
        ods output association=&tablain._stats;
    run;
    ods select all;

    %if &get_intercept = 0 %then %do;
        data &tablain._betas;
            set &tablain._betas;
            where Variable ^= "Intercept";
        run;
    %end;

%mend;

/*--------------------------------------------------------------
  Version: 2.1
  Módulo: Bootstrapping Report
  Output: Excel con 2 hojas
    1. CUBO: Tablón completo de betas por iteración
    2. GRAFICOS: Distribución por variable
--------------------------------------------------------------*/

%include "&_root_path/Sources/Modulos/m_bootstrapping/bootstrapping_macro.sas";

%macro __bootstraping_report(tablain=, tablaoot=, nrounds=, lista_variables=, target=);

    /* Ejecutar bootstrap */
    %__boots(
        tablain=&tablain., 
        tablaoot=&tablaoot., 
        nrounds=&nrounds., 
        lista_variables=&lista_variables., 
        target=&target.
    );

    /* Crear Excel con múltiples hojas */
    ods excel file="&&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._Bootstrapping.xlsx" 
        options(embedded_titles="yes");

    /*===========================================
      HOJA 1: CUBO - Tablón para validadores
      Todas las iteraciones en formato wide
    ===========================================*/
    ods excel options(sheet_name="BOOTS_ITERACIONES" sheet_interval="none");
    
    title "Bootstrapping - Betas por Iteración";    
    proc print data=cubo_wide noobs label;
    run;
    
    title;

    /*===========================================
      HOJA 2: CUBO LONG - Formato largo para pivot
    ===========================================*/
    ods excel options(sheet_name="BOOTS_CUBO" sheet_interval="now");
    
    title "Formato largo";
    
    proc print data=tablaout noobs;
    run;
    
    title;
    /*===========================================
      HOJA 3: RESUMEN - Estadísticas y alertas
    ===========================================*/
    ods excel options(sheet_name="RESUMEN_ESTABILIDAD" sheet_interval="now");
    
    title "Resumen de Estabilidad de Coeficientes";
    *title2 "Análisis de cambio de signo en &nrounds. iteraciones bootstrap";
    
    proc print data=report_final noobs label;
        var Variable 
            beta_dev beta_oot 
            flag_signo pct_signo_consistente alerta_signo
            n_positivos n_negativos
            beta_mean beta_std 
            p5 p25 p50 p75 p95
            beta_min beta_max
            pval_dev pval_oot
            peso_dev peso_oot;
        label 
            Variable = "Variable"
            beta_dev = "Beta TRAIN"
            beta_oot = "Beta OOT"
            flag_signo = "Estabilidad Signo"
            pct_signo_consistente = "% Consistencia"
            alerta_signo = "Alerta"
            n_positivos = "# Iter Positivas"
            n_negativos = "# Iter Negativas"
            beta_mean = "Media Bootstrap"
            beta_std = "Desv Est Bootstrap"
            p5 = "Percentil 5"
            p25 = "Percentil 25"
            p50 = "Mediana"
            p75 = "Percentil 75"
            p95 = "Percentil 95"
            beta_min = "Mínimo"
            beta_max = "Máximo"
            pval_dev = "P-Value TRAIN"
            pval_oot = "P-Value OOT"
            peso_dev = "Peso TRAIN"
            peso_oot = "Peso OOT";
    run;
    
    title;

    /*===========================================
      HOJA 4: GRAFICOS - Distribución por variable
    ===========================================*/
    ods listing gpath="&&path_troncal_&tr/&_img_path";
    ods graphics / imagename="tro_&tr._seg_&seg._Bootstrapping_" imagefmt=png;

    ods excel options(sheet_name="GRAFICOS" sheet_interval="now");
    
    /* Gráfico resumen: todas las variables */
    title "Intervalos Bootstrap vs Betas TRAIN/OOT";
    %__plot_boots(alias=&tablain.);
    
    /* Gráficos individuales por variable */
    %__plot_boots_by_var(data_iter=tablaout, data_report=report_final);

    ods excel close;
    ods graphics / reset;
    ods graphics off;
    /* Mensaje de confirmación */
    %put NOTE: ========================================;
    %put NOTE: Bootstrapping Report Generado;
    %put NOTE: Archivo: &&path_troncal_&tr/&_excel_path/tro_&tr._seg_&seg._Bootstrapping.xlsx;
    %put NOTE: Hojas:;
    %put NOTE:   1. CUBO_ITERACIONES - Formato wide para análisis;
    %put NOTE:   2. CUBO_LONG - Formato largo para pivot tables;
    %put NOTE:   3. RESUMEN_ESTABILIDAD - Estadísticas y alertas;
    %put NOTE:   4. GRAFICOS - Distribuciones por variable;
    %put NOTE: ========================================;

    /* Limpiar datasets temporales */
    proc datasets nolist;
        delete cubo_wide tablaout report_final pesos_report;
    run;

%mend;
/*---------------------------------------------------------------------------
  Version: 2.0	  
  Desarrollador: Joseph Chombo					
  Fecha Release: 01/09/2025
-----------------------------------------------------------------------------*/

%include "&_root_path/Sources/Modulos/m_bootstrapping/bootstrapping_report.sas";

%macro verify_bootstraping(data1, data2);
    %if %sysfunc(exist(&data1)) and %sysfunc(exist(&data2)) %then %do;
        %if %length(&vars_num.) > 0 and %length(&_target.) > 0 %then %do;
            %let v_nrounds = %sysfunc(ifc(%length(&_nrounds)=0, 1, &_nrounds));
            %__bootstraping_report(tablain=&data1, tablaoot=&data2, nrounds=&v_nrounds, lista_variables=&vars_num., target=&_target.);
        %end;
        %else %do;
            %put WARNING: (Bootstraping) No se ejecutará porque faltan variables numericas o target;
        %end;
    %end;
    %else %do;
        %put WARNING: (Bootstraping) No se ejecutara porque falta alguno de los datasets;
    %end;
%mend;