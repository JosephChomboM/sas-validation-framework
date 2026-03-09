/* =========================================================================
similitud_compute.sas - Computo de analisis de similitud de muestras

Contiene macros que:
A) Discretizan variables numericas via PROC RANK y generan graficos
   de distribucion evolutiva (stacked bar por bucket y periodo).
B) Comparan TRAIN vs OOT estadisticamente:
   - Numericas: mediana (MAE, RMSE, diferencia %)
   - Categoricas: moda (frecuencia %, diferencia)

Macros:
%_simil_calcular_cortes    - Puntos de corte via PROC RANK (Pattern B)
%_simil_bucket_plot        - Stacked bar por bucket y periodo (Pattern B)
%_simil_bucket_variables   - Orquestador bucket: itera vars num+cat
%_simil_similitud_num      - Comparacion mediana TRAIN vs OOT (Pattern B)
%_simil_similitud_cat      - Comparacion moda TRAIN vs OOT (Pattern B)

Todas las operaciones usan Pattern B (work staging):
PROC RANK, PROC SORT, PROC FREQ BY, PROC TRANSPOSE no son
CAS-compatibles.

Patron de cortes (TRAIN -> OOT):
- Numericas: cortes se calculan con TRAIN (reuse_cuts=0).
- OOT: reutiliza cortes de TRAIN (reuse_cuts=1, tabla work._simil_cortes).
- Categoricas: no se usan cortes (flg_continue=0).
========================================================================= */

/* =====================================================================
%_simil_calcular_cortes - Calcula cortes via PROC RANK + etiquetas
Crea tabla work._simil_cortes con: VARIABLE, RANGO, INICIO, FIN, ETIQUETA
===================================================================== */
%macro _simil_calcular_cortes(tablain, var, groups);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    /* Copiar datos a work para PROC RANK (solo columna necesaria) */
    data work._simil_rk_&rnd._1;
        set &tablain.(keep=&var.);
        &var. = round(&var., 0.0001);
    run;

    proc rank data=work._simil_rk_&rnd._1 out=work._simil_rk_&rnd._2
        groups=&groups.;
        ranks RANGO;
        var &var.;
    run;

    proc sql noprint;
        create table work._simil_rk_&rnd._3 as
        select RANGO,
               min(&var.) as MINVAL,
               max(&var.) as MAXVAL
        from work._simil_rk_&rnd._2
        group by RANGO;
    quit;

    proc sort data=work._simil_rk_&rnd._3;
        by RANGO;
    run;

    data work._simil_rk_&rnd._4;
        set work._simil_rk_&rnd._3(rename=(RANGO=RANGO_INI)) end=EOF;
        retain MARCA 0;
        N = _n_;
        FLAG_INI = 0;
        FLAG_FIN = 0;
        LAGMAXVAL = lag(MAXVAL);
        RANGO = RANGO_INI + 1;
        if RANGO_INI = . then RANGO = 0;
        if RANGO_INI >= 0 then MARCA = MARCA + 1;
        if MARCA = 1 then FLAG_INI = 1;
        if EOF then FLAG_FIN = 1;
    run;

    proc sql noprint;
        create table work._simil_cortes as
        select "&var." as VARIABLE length=32,
               RANGO,
               RANGO_INI,
               LAGMAXVAL as INICIO,
               MAXVAL as FIN,
               FLAG_INI,
               FLAG_FIN,
               case
                   when RANGO = 0 then "00. Missing"
                   when FLAG_INI = 1 then
                       cat(put(RANGO, Z2.), ". <-Inf; ",
                           cats(put(MAXVAL, F12.4)), "]")
                   when FLAG_FIN = 1 then
                       cat(put(RANGO, Z2.), ". <",
                           cats(put(LAGMAXVAL, F12.4)), "; +Inf>")
                   else
                       cat(put(RANGO, Z2.), ". <",
                           cats(put(LAGMAXVAL, F12.4)), "; ",
                           cats(put(MAXVAL, F12.4)), "]")
               end as ETIQUETA length=200
        from work._simil_rk_&rnd._4;
    quit;

    /* Cleanup staging */
    proc datasets library=work nolist nowarn;
        delete _simil_rk_&rnd.:;
    quit;

%mend _simil_calcular_cortes;

/* =====================================================================
%_simil_bucket_plot - Stacked bar de distribucion por bucket y periodo

Para numericas (flg_continue=1):
- Discretiza con cortes de work._simil_cortes
- Genera stacked bar (bucket % por periodo) + tabla pivotada

Para categoricas (flg_continue=0):
- Agrupa directamente por valor de la variable
- Genera stacked bar (categoria % por periodo) + tabla pivotada

Patron: Pattern B (work staging) - PROC SORT, PROC FREQ BY,
PROC TRANSPOSE no son CAS-compatibles.
===================================================================== */
%macro _simil_bucket_plot(tablain, var, byvar=, groups=5, flg_continue=1,
    reuse_cuts=0, m_data_type=);

    %local rnd DATAAPPLY;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    %let color_list=mediumblue salmon mediumpurple gold vligb palegreen
        pab vigb lip vpab;

    %if &flg_continue. = 1 %then %do;
        /* --- Numerica: discretizar con cortes --- */

        /* Copiar datos a work, reemplazar dummies con missing */
        data work._simil_bp_&rnd._0;
            set &tablain.;
            if &var. in (., 1111111111, -1111111111, 2222222222,
                -2222222222, 3333333333, -3333333333, 4444444444,
                5555555555, 6666666666, 7777777777, -999999999)
                then &var. = .;
        run;

        /* Calcular cortes si no reutilizamos */
        %if &reuse_cuts. = 0 %then %do;
            %_simil_calcular_cortes(work._simil_bp_&rnd._0, &var., &groups.);

            proc sort data=work._simil_cortes;
                by RANGO;
            run;
        %end;
        /* Si reuse_cuts=1, work._simil_cortes ya existe de TRAIN */

        /* Construir sentencias IF para etiquetar buckets */
        data work._simil_bp_&rnd._1;
            set work._simil_cortes end=EOF;
            length QUERY_BODY $500;
            if RANGO = 0 then
                QUERY_BODY = cat("IF MISSING(&var.)=1 THEN ETIQUETA=",
                    '"', strip(ETIQUETA), '";');
            else if FLAG_INI = 1 then
                QUERY_BODY = cat("IF &var.<=", FIN,
                    " AND &var.>. THEN ETIQUETA=",
                    '"', strip(ETIQUETA), '";');
            else if FLAG_FIN = 1 then
                QUERY_BODY = cat("IF &var.>", INICIO,
                    " THEN ETIQUETA=",
                    '"', strip(ETIQUETA), '";');
            else
                QUERY_BODY = cat("IF ", INICIO, "<&var.<=", FIN,
                    " THEN ETIQUETA=",
                    '"', strip(ETIQUETA), '";');
        run;

        /* Color map para stacked bars */
        data work._simil_attrmap_&rnd.(keep=id value fillcolor);
            length FILLCOLOR $20;
            set work._simil_bp_&rnd._1;
            ID = 'MYID';
            VALUE = ETIQUETA;
            FILLCOLOR = scan("&color_list.", RANGO);
        run;

        proc sql noprint;
            select QUERY_BODY into :DATAAPPLY separated by " "
            from work._simil_bp_&rnd._1;
        quit;

        /* Aplicar etiquetas a datos */
        data work._simil_bp_&rnd._2;
            set work._simil_bp_&rnd._0;
            length ETIQUETA $200;
            &DATAAPPLY.;
        run;

        /* Calcular frecuencias por periodo y bucket */
        proc sort data=work._simil_bp_&rnd._2;
            by &byvar.;
        run;

        proc freq data=work._simil_bp_&rnd._2 noprint;
            by &byvar.;
            tables ETIQUETA / out=work._simil_bp_&rnd._3;
        run;

        title "Evolutivo distribucion variable &var. - &m_data_type.";

        proc sgplot data=work._simil_bp_&rnd._3
            dattrmap=work._simil_attrmap_&rnd.;
            vbar &byvar. / response=percent group=ETIQUETA
                groupdisplay=stack nooutline name="bars"
                attrid=MYID barwidth=1;
            keylegend "bars" / title="Rango" opaque;
            xaxis type=discrete discreteorder=data
                valueattrs=(size=7pt);
        run;
        title;

        /* Tabla pivotada de porcentajes */
        proc transpose data=work._simil_bp_&rnd._3
            out=work._simil_bp_&rnd._rpt(drop=_name_ _label_);
            by &byvar.;
            id ETIQUETA;
            var percent;
        run;

        ods escapechar="^";
        ods text=" ";
        ods text="^S={fontweight=bold fontsize=11pt} Bucket % de &var. por &byvar.";
        ods text=" ";

        proc print data=work._simil_bp_&rnd._rpt noobs;
        run;

    %end;
    %else %do;
        /* --- Categorica: agrupar directamente --- */

        /* Copiar datos a work */
        data work._simil_bp_&rnd._0;
            set &tablain.;
        run;

        /* Color map basado en valores distintos */
        proc sql noprint;
            create table work._simil_bp_&rnd._cats as
            select distinct &var. as category
            from work._simil_bp_&rnd._0
            order by &var.;
        quit;

        data work._simil_attrmap_&rnd.(keep=id value fillcolor);
            length FILLCOLOR $20;
            set work._simil_bp_&rnd._cats;
            ID = 'MYID';
            VALUE = cats(category);
            FILLCOLOR = scan("&color_list.", _N_);
        run;

        /* Frecuencias por periodo y categoria */
        proc sort data=work._simil_bp_&rnd._0;
            by &byvar.;
        run;

        proc freq data=work._simil_bp_&rnd._0 noprint;
            by &byvar.;
            tables &var. / out=work._simil_bp_&rnd._3;
        run;

        data work._simil_bp_&rnd._3;
            set work._simil_bp_&rnd._3;
            category = cats(&var.);
        run;

        title "Evolutivo distribucion variable &var. - &m_data_type.";

        proc sgplot data=work._simil_bp_&rnd._3
            dattrmap=work._simil_attrmap_&rnd.;
            vbar &byvar. / response=percent group=category
                groupdisplay=stack nooutline name="bars"
                attrid=MYID barwidth=1;
            keylegend "bars" / title="Rango" opaque;
            xaxis type=discrete discreteorder=data
                valueattrs=(size=7pt);
        run;
        title;

        /* Tabla pivotada */
        proc transpose data=work._simil_bp_&rnd._3
            out=work._simil_bp_&rnd._rpt(drop=_name_ _label_);
            by &byvar.;
            id &var.;
            var percent;
        run;

        ods escapechar="^";
        ods text=" ";
        ods text="^S={fontweight=bold fontsize=11pt} Bucket % de &var. por &byvar.";
        ods text=" ";

        proc print data=work._simil_bp_&rnd._rpt noobs;
        run;

    %end;

    /* Cleanup staging */
    proc datasets library=work nolist nowarn;
        delete _simil_bp_&rnd.: _simil_attrmap_&rnd.;
    quit;

%mend _simil_bucket_plot;

/* =====================================================================
%_simil_bucket_variables - Orquestador: itera vars num+cat para buckets

Para cada variable numerica:
- TRAIN: calcula cortes (reuse_cuts=0)
- OOT: reutiliza cortes de TRAIN (reuse_cuts=1)

Para cada variable categorica:
- TRAIN + OOT sin cortes (flg_continue=0)
===================================================================== */
%macro _simil_bucket_variables(train_data=, oot_data=, byvar=, vars_num=,
    vars_cat=, groups=5);

    %local c v z v_cat;

    /* Procesar variables numericas */
    %if %length(&vars_num.) > 0 %then %do;
        %let c = 1;
        %let v = %scan(&vars_num., &c., %str( ));
        %do %while(%length(&v.) > 0);
            %put NOTE: [similitud] Bucket plot numerica: &v.;

            /* TRAIN: calcular cortes */
            %_simil_bucket_plot(&train_data., &v., byvar=&byvar.,
                groups=&groups., flg_continue=1, reuse_cuts=0,
                m_data_type=TRAIN);

            /* OOT: reusar cortes de TRAIN */
            %_simil_bucket_plot(&oot_data., &v., byvar=&byvar.,
                groups=&groups., flg_continue=1, reuse_cuts=1,
                m_data_type=OOT);

            /* Limpiar cortes despues de OOT */
            proc datasets library=work nolist nowarn;
                delete _simil_cortes;
            quit;

            %let c = %eval(&c. + 1);
            %let v = %scan(&vars_num., &c., %str( ));
        %end;
    %end;

    /* Procesar variables categoricas */
    %if %length(&vars_cat.) > 0 %then %do;
        %let z = 1;
        %let v_cat = %scan(&vars_cat., &z., %str( ));
        %do %while(%length(&v_cat.) > 0);
            %put NOTE: [similitud] Bucket plot categorica: &v_cat.;

            /* TRAIN */
            %_simil_bucket_plot(&train_data., &v_cat., byvar=&byvar.,
                groups=&groups., flg_continue=0, reuse_cuts=0,
                m_data_type=TRAIN);

            /* OOT */
            %_simil_bucket_plot(&oot_data., &v_cat., byvar=&byvar.,
                groups=&groups., flg_continue=0, reuse_cuts=0,
                m_data_type=OOT);

            %let z = %eval(&z. + 1);
            %let v_cat = %scan(&vars_cat., &z., %str( ));
        %end;
    %end;

%mend _simil_bucket_variables;

/* =====================================================================
%_simil_similitud_num - Comparacion de medianas TRAIN vs OOT
(variables numericas)

Calcula: Mediana_TRAIN, Mediana_OOT, MAE, RMSE, Diferencia %
Semaforo: Alta Similitud (<umbral_verde), Similitud Media, Baja Similitud

Pattern B: PROC MEANS output, PROC APPEND, PROC PRINT en work.
===================================================================== */
%macro _simil_similitud_num(train_data=, oot_data=, vars_num=, target=,
    umbral_verde=10, umbral_amarillo=20);

    %local todas_vars total_vars i var_num;
    %local mediana_train mediana_oot mae rmse diferencia_pct similitud;

    /* Combinar target + variables numericas */
    %let todas_vars = &target. &vars_num.;

    %if %length(%superq(todas_vars)) = 0 %then %do;
        %put WARNING: [similitud] No hay variables numericas para similitud.;
        %return;
    %end;

    /* Crear tabla de resultados vacia */
    data work._simil_res_num;
        length Variable $32 Mediana_TRAIN 8 Mediana_OOT 8 MAE 8 RMSE 8
            Diferencia_Pct 8 Similitud $20;
        stop;
    run;

    %let total_vars = %sysfunc(countw(&todas_vars., %str( )));

    %do i = 1 %to &total_vars.;
        %let var_num = %scan(&todas_vars., &i., %str( ));
        %put NOTE: [similitud] Similitud numerica: &var_num.;

        /* Mediana TRAIN */
        proc means data=&train_data. median noprint;
            var &var_num.;
            output out=work._simil_med_trn(drop=_type_ _freq_)
                median=mediana;
        run;

        data _null_;
            set work._simil_med_trn;
            call symputx('mediana_train', mediana);
        run;

        /* Mediana OOT */
        proc means data=&oot_data. median noprint;
            var &var_num.;
            output out=work._simil_med_oot(drop=_type_ _freq_)
                median=mediana;
        run;

        data _null_;
            set work._simil_med_oot;
            call symputx('mediana_oot', mediana);
        run;

        /* Calcular metricas de error */
        %let mae = %sysfunc(abs(%sysevalf(&mediana_train. - &mediana_oot.)));
        %let rmse = %sysfunc(sqrt((&mediana_train. - &mediana_oot.)**2));

        /* Diferencia porcentual (evitar division por cero) */
        %if %sysevalf(&mediana_train. ^= 0) %then %do;
            %let diferencia_pct = %sysevalf(100 * &mae. /
                %sysfunc(abs(&mediana_train.)));
        %end;
        %else %if %sysevalf(&mediana_oot. = 0) %then %do;
            %let diferencia_pct = 0;
        %end;
        %else %do;
            %let diferencia_pct = 100;
        %end;

        /* Semaforo */
        %if %sysevalf(&diferencia_pct. < &umbral_verde.) %then
            %let similitud = Alta Similitud;
        %else %if %sysevalf(&diferencia_pct. < &umbral_amarillo.) %then
            %let similitud = Similitud Media;
        %else
            %let similitud = Baja Similitud;

        /* Agregar fila al resultado */
        data work._simil_tmp_num;
            length Variable $32 Mediana_TRAIN 8 Mediana_OOT 8 MAE 8 RMSE 8
                Diferencia_Pct 8 Similitud $20;
            Variable = "&var_num.";
            Mediana_TRAIN = &mediana_train.;
            Mediana_OOT = &mediana_oot.;
            MAE = &mae.;
            RMSE = &rmse.;
            Diferencia_Pct = &diferencia_pct.;
            Similitud = "&similitud.";
        run;

        proc append base=work._simil_res_num data=work._simil_tmp_num;
        run;
    %end;

    /* Formato semaforo */
    proc format;
        value $simil_bg
            'Alta Similitud' = 'lightgreen'
            'Similitud Media' = 'yellow'
            'Baja Similitud' = 'salmon';
    run;

    title "Similitud de muestras - Variables Numericas (Mediana)";

    proc print data=work._simil_res_num label noobs;
        var Variable Mediana_TRAIN Mediana_OOT MAE RMSE Diferencia_Pct;
        var Similitud / style={background=$simil_bg.};
        format Mediana_TRAIN Mediana_OOT 12.4 MAE RMSE 12.4
            Diferencia_Pct 8.1;
        label MAE = "Error Abs. Medio"
              RMSE = "Raiz Error Cuad."
              Diferencia_Pct = "Diferencia (%)"
              Similitud = "Nivel de Similitud";
    run;
    title;

    /* Cleanup */
    proc datasets library=work nolist nowarn;
        delete _simil_res_num _simil_tmp_num _simil_med_trn _simil_med_oot;
    quit;

%mend _simil_similitud_num;

/* =====================================================================
%_simil_similitud_cat - Comparacion de modas TRAIN vs OOT
(variables categoricas)

Calcula: Moda_TRAIN, Moda_OOT, Pct_TRAIN, Pct_OOT, Diferencia %
Semaforo: Alta Similitud (<umbral_verde), Similitud Media, Baja Similitud

Pattern B: PROC FREQ, PROC SORT, PROC APPEND, PROC PRINT en work.
===================================================================== */
%macro _simil_similitud_cat(train_data=, oot_data=, vars_cat=,
    umbral_verde=10, umbral_amarillo=20);

    %local total_vars i var_cat;
    %local moda_train moda_oot pct_train pct_oot diferencia similitud;

    %if %length(%superq(vars_cat)) = 0 %then %do;
        %put WARNING: [similitud] No hay variables categoricas para similitud.;
        %return;
    %end;

    /* Crear tabla de resultados vacia */
    data work._simil_res_cat;
        length Variable $32 Moda_TRAIN $100 Moda_OOT $100 Pct_TRAIN 8
            Pct_OOT 8 Diferencia 8 Similitud $20;
        stop;
    run;

    %let total_vars = %sysfunc(countw(&vars_cat., %str( )));

    %do i = 1 %to &total_vars.;
        %let var_cat = %scan(&vars_cat., &i., %str( ));
        %put NOTE: [similitud] Similitud categorica: &var_cat.;

        /* Moda TRAIN */
        proc freq data=&train_data. noprint;
            tables &var_cat. / out=work._simil_freq_trn missing;
        run;

        data work._simil_freq_trn;
            set work._simil_freq_trn;
            where not missing(&var_cat.);
        run;

        proc sort data=work._simil_freq_trn;
            by descending count;
        run;

        data _null_;
            set work._simil_freq_trn(obs=1);
            call symputx('moda_train', &var_cat.);
            call symputx('pct_train', percent);
        run;

        /* Moda OOT */
        proc freq data=&oot_data. noprint;
            tables &var_cat. / out=work._simil_freq_oot missing;
        run;

        data work._simil_freq_oot;
            set work._simil_freq_oot;
            where not missing(&var_cat.);
        run;

        proc sort data=work._simil_freq_oot;
            by descending count;
        run;

        data _null_;
            set work._simil_freq_oot(obs=1);
            call symputx('moda_oot', &var_cat.);
            call symputx('pct_oot', percent);
        run;

        /* Diferencia y semaforo */
        %let diferencia = %sysfunc(abs(%sysevalf(&pct_train. - &pct_oot.)));

        %if %sysevalf(&diferencia. < &umbral_verde.) %then
            %let similitud = Alta Similitud;
        %else %if %sysevalf(&diferencia. < &umbral_amarillo.) %then
            %let similitud = Similitud Media;
        %else
            %let similitud = Baja Similitud;

        /* Agregar fila al resultado */
        data work._simil_tmp_cat;
            length Variable $32 Moda_TRAIN $100 Moda_OOT $100 Pct_TRAIN 8
                Pct_OOT 8 Diferencia 8 Similitud $20;
            Variable = "&var_cat.";
            Moda_TRAIN = "&moda_train.";
            Moda_OOT = "&moda_oot.";
            Pct_TRAIN = &pct_train.;
            Pct_OOT = &pct_oot.;
            Diferencia = &diferencia.;
            Similitud = "&similitud.";
        run;

        proc append base=work._simil_res_cat data=work._simil_tmp_cat;
        run;
    %end;

    /* Formato semaforo */
    proc format;
        value $simil_bg
            'Alta Similitud' = 'lightgreen'
            'Similitud Media' = 'yellow'
            'Baja Similitud' = 'salmon';
    run;

    title "Similitud de muestras - Variables Categoricas (Moda)";

    proc print data=work._simil_res_cat label noobs;
        var Variable Moda_TRAIN Pct_TRAIN Moda_OOT Pct_OOT Diferencia;
        var Similitud / style={background=$simil_bg.};
        format Pct_TRAIN Pct_OOT 8.1 Diferencia 8.1;
        label Diferencia = "Diferencia (%)"
              Similitud = "Nivel de Similitud";
    run;
    title;

    /* Cleanup */
    proc datasets library=work nolist nowarn;
        delete _simil_res_cat _simil_tmp_cat _simil_freq_trn _simil_freq_oot;
    quit;

%mend _simil_similitud_cat;
