/*==============================================================
SECCION 1: MACROS BASE DE CALCULO
==============================================================*/

/*--------------------------------------------------------------
Macro: __gini_calc
Descripción: Cálculo base del coeficiente GINI (Somers' D)
Output: Macro variable &out_gini
--------------------------------------------------------------*/
%macro __gini_calc(data=, target=, var=, out_gini=gini_valor);

    %global &out_gini.;
    %let &out_gini.=.;

    %local rnd dsid rc;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    /* Verificar que el dataset existe */
    %if not %sysfunc(exist(&data.)) %then %do;
        %put WARNING: [GINI] Dataset no existe: &data.;
        %return;
    %end;

    /* Verificar que las variables existen */
    %let dsid=%sysfunc(open(&data.));
    %if &dsid. > 0 %then %do;
        %if %sysfunc(varnum(&dsid., &target.))=0 %then %do;
            %put WARNING: [GINI] Variable target no existe: &target.;
            %let rc=%sysfunc(close(&dsid.));
            %return;
        %end;
        %if %sysfunc(varnum(&dsid., &var.))=0 %then %do;
            %put WARNING: [GINI] Variable score no existe: &var.;
            %let rc=%sysfunc(close(&dsid.));
            %return;
        %end;
        %let rc=%sysfunc(close(&dsid.));
    %end;

    /* Intentar PROC LOGISTIC con ridging por defecto */
    ods select none;

    proc logistic data=&data.;
        model &target. (event="1")=&var.;
        ods output association=_gini_assoc_&rnd.;
    run;
    ods select all;

    /* Si falla, reintentar con RIDGING=NONE */
    %if not %sysfunc(exist(_gini_assoc_&rnd.)) %then %do;
        %put NOTE: [GINI] Reintentando &var. con RIDGING=NONE;
        ods select none;

        proc logistic data=&data.;
            model &target. (event="1")=&var. / RIDGING=NONE;
            ods output association=_gini_assoc_&rnd.;
        run;
        ods select all;
    %end;

    /* Extraer Somers' D (GINI) */
    %if %sysfunc(exist(_gini_assoc_&rnd.)) %then %do;
        proc sql noprint;
            select abs(nvalue2) into :&out_gini. trimmed from _gini_assoc_&rnd.
                where substr(label2, 1, 6)="Somers";
        quit;
    %end;
    %else %do;
        %put WARNING: [GINI] LOGISTIC falló para variable &var.;
    %end;

    /* Limpiar */
    proc datasets lib=work nolist nowarn;
        delete _gini_assoc_&rnd.;
    quit;

%mend __gini_calc;


/*--------------------------------------------------------------
Macro auxiliar: __gini_by_period
Descripción: Calcular GINI por periodo usando BY statement
--------------------------------------------------------------*/
%macro __gini_by_period( data=, target=, score=, time_var=, dataset_name=);
    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    /* Ordenar por periodo */
    proc sort data=&data. out=_gini_by_&rnd.;
        by &time_var.;
    run;

    /* Calcular GINI por periodo usando BY */
    ods select none;

    proc logistic data=_gini_by_&rnd.;
        model &target. (event="1")=&score.;
        by &time_var.;
        ods output Association=_gini_assoc_&rnd.;
    run;
    ods select all;

    /* Extraer Somers' D (GINI) */
    data _gini_result_&rnd.(keep=&time_var. Gini);
        set _gini_assoc_&rnd.;
        where substr(Label2, 1, 6)="Somers";
        Gini=abs(nValue2);
    run;

    /* Obtener conteos por periodo */
    proc sql;
        create table _gini_stats_&rnd. as select &time_var., count(*) as N,
            sum(&target.) as N_Default from _gini_by_&rnd. group by &time_var.;
    quit;

    /* Combinar y agregar al CUBO */
    proc sql;
        insert into cubo_gini_modelo select "&dataset_name." as Dataset,
            a.&time_var. as Periodo, a.N, a.N_Default, (a.N_Default / a.N) as
            Tasa_Default format=percent8.2, b.Gini format=8.4, "" as Tendencia
            from _gini_stats_&rnd. a left join _gini_result_&rnd. b on
            a.&time_var.=b.&time_var.;
    quit;

    /* Limpieza */
    proc datasets lib=work nolist nowarn;
        delete _gini_by_&rnd. _gini_assoc_&rnd. _gini_result_&rnd.
            _gini_stats_&rnd.;
    quit;

%mend __gini_by_period;


/*--------------------------------------------------------------
Macro auxiliar: __gini_var_all_periods
Descripción: Calcular GINI de UNA variable en TODOS los periodos
--------------------------------------------------------------*/
%macro __gini_var_all_periods( data=, target=, var=, time_var=, dataset_name=,
    use_weights=1, def_close=0 );
    %local rnd max_desempeno dsid nobs rc has_data;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));
    %let has_data=0;

    /* Preparar datos con filtro de cierre si aplica */
    %if &def_close. > 0 %then %do;
        proc sql noprint;
            select max(&time_var.) into :max_periodo trimmed from &data.;
        quit;

        %let max_desempeno=%sysfunc(intnx(MONTH,
            %sysfunc(inputn(&max_periodo.01, yymmdd8.)), -&def_close., B));
        %let max_desempeno=%sysfunc(putn(&max_desempeno., yymmn6.));

        data _vd_&rnd.;
            set &data.(keep=&time_var. &target. &var. where=(&var. is not
                missing));
            where &time_var. <= &max_desempeno.;
            _wt_=1;
        run;
    %end;
    %else %do;
        data _vd_&rnd.;
            set &data.(keep=&time_var. &target. &var. where=(&var. is not
                missing));
            _wt_=1;
        run;
    %end;

    /* Verificar datos */
    %let dsid=%sysfunc(open(_vd_&rnd.));
    %if &dsid. %then %do;
        %let nobs=%sysfunc(attrn(&dsid., nobs));
        %let rc=%sysfunc(close(&dsid.));
        %if &nobs. > 0 %then %let has_data=1;
    %end;

    %if &has_data.=0 %then %do;
        %put WARNING: [GINI VAR MENSUAL] Variable &var. sin datos validos en
            &dataset_name.;

        proc datasets lib=work nolist nowarn;
            delete _vd_&rnd.;
        quit;
        %return;
    %end;

    /* Ordenar antes del merge */
    proc sort data=_vd_&rnd.;
        by &time_var.;
    run;

    /* Ponderación balanceada por periodo (si aplica) */
    %if &use_weights.=1 %then %do;
        proc sql;
            create table _wt_&rnd. as select &time_var., sum(&target.) as n1,
                sum(1-&target.) as n0, case when calculated n1 > 0 then
                calculated n0 / calculated n1 else 1 end as mult from _vd_&rnd.
                group by &time_var.;
        quit;

        proc sort data=_wt_&rnd.;
            by &time_var.;
        run;

        data _vd_&rnd.;
            merge _vd_&rnd. _wt_&rnd.(keep=&time_var. mult);
            by &time_var.;
            if &target.=1 then _wt_=coalesce(mult, 1);
            else _wt_=1;
            drop mult;
        run;
    %end;

    /* GINI para TODOS los periodos - un solo PROC LOGISTIC */
    ods select none;

    proc logistic data=_vd_&rnd. namelen=60;
        by &time_var.;
        model &target.(event='1')=&var.;
        weight _wt_;
        ods output ParameterEstimates=_pe_&rnd.(where=(Variable ne 'Intercept'))
            Association=_as_&rnd.(where=(Label2 =: 'Somers'));
    run;
    ods select all;

    /* Conteos por periodo */
    proc sql;
        create table _ct_&rnd. as select &time_var., count(*) as N, count(&var.)
            as N_Valid, sum(&target.) as N_Default from _vd_&rnd. group by
            &time_var.;
    quit;

    /* Insertar en CUBO */
    proc sql;
        insert into cubo_gini_variables select "&var." as Variable,
            "&dataset_name." as Dataset, c.&time_var. as Periodo, c.N,
            c.N_Valid, c.N_Default, coalesce(p.Estimate, .) as Estimate,
            coalesce(p.WaldChiSq, .) as WaldChiSq, coalesce(p.ProbChiSq, .) as
            ProbChiSq, coalesce(abs(a.nValue2), .) as Gini from _ct_&rnd. c left
            join _pe_&rnd. p on c.&time_var.=p.&time_var. left join _as_&rnd. a
            on c.&time_var.=a.&time_var.;
    quit;

    /* Limpieza */
    proc datasets lib=work nolist nowarn;
        delete _vd_&rnd. _wt_&rnd. _pe_&rnd. _as_&rnd. _ct_&rnd.;
    quit;

%mend __gini_var_all_periods;
/*--------------------------------------------------------------
Macro: __gini_modelo_m
Descripción: Calcula GINI global del modelo para TRAIN y OOT
Output: Dataset gini_modelo
--------------------------------------------------------------*/
%include "&_root_path./Sources/Modulos/m_gini/__aux_gini_utils.sas";

%macro __gini_modelo_m(
    t1=, /* Dataset TRAIN */
    t2=, /* Dataset OOT */ 
    target=, /* Variable target (0/1) */ 
    score=, /* Variable score (PD o XB) */
    param_model_type=APP /* APP o BHV para umbrales */
);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    /* Inicializar tabla resultado */
    proc sql;
        create table gini_modelo ( Dataset char(10), N_Obs num, N_Default num,
            N_No_Default num, Tasa_Default num format=percent8.2, Gini num
            format=8.4, Gini_Min num format=8.4, Gini_Max num format=8.4,
            IC_95_Lower num format=8.4, IC_95_Upper num format=8.4 );
    quit;

    /*=========================================
    1. Calcular GINI para TRAIN
    =========================================*/
    %if %sysfunc(exist(&t1.)) %then %do;

        /* Estadísticos básicos */
        proc sql noprint;
            select count(*), sum(&target.), sum(1 - &target.) into :n_train
                trimmed, :def_train trimmed, :nodef_train trimmed from &t1.;
        quit;

        /* Calcular GINI */
        %__gini_calc(data=&t1., target=&target., var=&score.,
            out_gini=gini_train);

        /* Calcular IC 95% aproximado */
        /* IC aproximado: Gini ± 1.96 * SE, donde SE ≈ sqrt(Gini*(1-Gini)/n) */
        %let se_train=%sysevalf(%sysfunc(sqrt(&gini_train. * (1 - &gini_train.)
            / &n_train.)));
        %let ic_lower_train=%sysevalf(&gini_train. - 1.96 * &se_train.);
        %let ic_upper_train=%sysevalf(&gini_train. + 1.96 * &se_train.);
        %let tasa_train=%sysevalf(&def_train. / &n_train.);

        /* Insertar resultado TRAIN */
        proc sql;
            insert into gini_modelo values ( "TRAIN", &n_train., &def_train.,
                &nodef_train., &tasa_train., &gini_train., &ic_lower_train.,
                &ic_upper_train., &ic_lower_train., &ic_upper_train. );
        quit;
    %end;

    /*=========================================
    2. Calcular GINI para OOT
    =========================================*/
    %if %sysfunc(exist(&t2.)) %then %do;

        /* Estadísticos básicos */
        proc sql noprint;
            select count(*), sum(&target.), sum(1 - &target.) into :n_oot
                trimmed, :def_oot trimmed, :nodef_oot trimmed from &t2.;
        quit;

        /* Calcular GINI */
        %__gini_calc(data=&t2., target=&target., var=&score.,
            out_gini=gini_oot);

        /* IC 95% aproximado */
        %let se_oot=%sysevalf(%sysfunc(sqrt(&gini_oot. * (1 - &gini_oot.) /
            &n_oot.)));
        %let ic_lower_oot=%sysevalf(&gini_oot. - 1.96 * &se_oot.);
        %let ic_upper_oot=%sysevalf(&gini_oot. + 1.96 * &se_oot.);
        %let tasa_oot=%sysevalf(&def_oot. / &n_oot.);

        /* Insertar resultado OOT */
        proc sql;
            insert into gini_modelo values ( "OOT", &n_oot., &def_oot.,
                &nodef_oot., &tasa_oot., &gini_oot., &ic_lower_oot.,
                &ic_upper_oot., &ic_lower_oot., &ic_upper_oot. );
        quit;
    %end;

    /*=========================================
    3. Agregar métricas comparativas
    =========================================*/
    %if %sysfunc(exist(&t1.)) and %sysfunc(exist(&t2.)) %then %do;
        data gini_modelo;
            set gini_modelo;
            /* Calcular degradación del GINI */
            if _n_=1 then do;
                Gini_Train=Gini;
                retain Gini_Train;
            end;
            if Dataset="OOT" then do;
                Degradacion=(Gini_Train - Gini) / Gini_Train;
                format Degradacion percent8.2;
            end;
            drop Gini_Train;
        run;
    %end;

    %put NOTE: [GINI MODELO] Cálculo completado;

%mend __gini_modelo_m;

/*--------------------------------------------------------------
Macro: __gini_modelo_mensual_m
Descripción: Calcula GINI del modelo por periodo para TRAIN/OOT
Output: cubo_gini_modelo
--------------------------------------------------------------*/
%macro __gini_modelo_mensual_m(
    t1=, /* Dataset TRAIN */ 
    t2=, /* Dataset OOT */
    target=, /* Variable target (0/1) */ 
    score=, /* Variable score (PD o XB) */
    time_var=/* Variable temporal*/
);

    %local rnd;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    /* Inicializar CUBO resultado */
    proc sql;
        create table cubo_gini_modelo ( Dataset char(10), Periodo num format=6.,
            N num, N_Default num, Tasa_Default num format=percent8.2, Gini num
            format=8.4, Tendencia char(15) );
    quit;

    /*=========================================
    1. Procesar TRAIN por periodo
    =========================================*/
    %if %sysfunc(exist(&t1.)) %then %do;
        %__gini_by_period( data=&t1., target=&target., score=&score.,
            time_var=&time_var., dataset_name=TRAIN );
    %end;

    /*=========================================
    2. Procesar OOT por periodo
    =========================================*/
    %if %sysfunc(exist(&t2.)) %then %do;
        %__gini_by_period( data=&t2., target=&target., score=&score.,
            time_var=&time_var., dataset_name=OOT );
    %end;

    /*=========================================
    3. Calcular tendencia general
    =========================================*/
    proc sql;
        create table _gini_first_last as select Dataset, min(Periodo) as
            First_Period, max(Periodo) as Last_Period from cubo_gini_modelo
            group by Dataset;
    quit;

    proc sql;
        create table _gini_trend_calc as select a.Dataset, b.Gini as Gini_First,
            c.Gini as Gini_Last, (c.Gini - b.Gini) as Delta_Gini format=8.4,
            case when (c.Gini - b.Gini) < -0.03 then "EMPEORANDO" when (c.Gini -
            b.Gini) > 0.03 then "MEJORANDO" else "ESTABLE" end as
            Tendencia_Global length=15 from _gini_first_last a left join
            cubo_gini_modelo b on a.Dataset=b.Dataset and a.First_Period=
            b.Periodo left join cubo_gini_modelo c on a.Dataset=c.Dataset and
            a.Last_Period=c.Periodo;
    quit;

    /* Actualizar tendencia en CUBO */
    proc sql;
        update cubo_gini_modelo set Tendencia=( select Tendencia_Global from
            _gini_trend_calc where _gini_trend_calc.Dataset=
            cubo_gini_modelo.Dataset );
    quit;

    /* Limpieza */
    proc datasets lib=work nolist;
        delete _gini_first_last _gini_trend_calc _gini_by_:;
    quit;

    %put NOTE: [GINI MODELO MENSUAL] Cálculo completado;

%mend __gini_modelo_mensual_m;
/*--------------------------------------------------------------
Módulo: GINI Report Consolidado
Versión: 4.0
Desarrollador: Joseph Chombo
Fecha: 2025-12-14

Descripción: Orquesta cálculos GINI y genera reportes Excel

Umbrales:
- Modelo APP: 0.40 (aceptable), 0.50 (satisfactorio)
- Modelo BHV: 0.50 (aceptable), 0.60 (satisfactorio)
- Variables: 0.05 (aceptable), 0.15 (satisfactorio)
--------------------------------------------------------------*/

%include "&_root_path./Sources/Modulos/m_gini/__gini_modelo_m.sas";
%include "&_root_path./Sources/Modulos/m_gini/__gini_variable_m.sas";
%include "&_root_path./Sources/Modulos/m_gini/__aux_gini_plots.sas";

%macro __gini_r(
    t1=, /* Dataset TRAIN */ 
    t2=, /* Dataset OOT */ 
    target=, /* Variable target (0/1) */ 
    score=, /* Variable score del modelo (PD o XB) */ 
    var_list=, /* Lista de variables predictoras */ 
    byvar=, /* Variable temporal (periodo YYYYMM) */ 
    model_type=APP /* Tipo de modelo: APP o BHV */ 
);

    %local thres_modelo thres_var fmt_modelo;

    /* Definir umbrales según tipo de modelo */
    %if %upcase(&model_type.)=BHV %then %let thres_modelo=0.50;
    %else %let thres_modelo=0.40;

    /* Umbral fijo para variables */
    %let thres_var=0.05;

    %put NOTE:================================================;
    %put NOTE: [GINI REPORT] Iniciando proceso;
    %put NOTE: Modelo: &model_type. | Umbral: &thres_modelo.;
    %put NOTE: Variables: &thres_var.;
    %put NOTE:================================================;

    /*=========================================
    1. GINI Global del Modelo
    =========================================*/
    %__gini_modelo_m( t1=&t1., t2=&t2., target=&target., score=&score.,
        param_model_type=&model_type. );

    /*=========================================
    2. GINI Modelo por Periodo
    =========================================*/
    %__gini_modelo_mensual_m( t1=&t1., t2=&t2., target=&target., score=&score.,
        time_var=&byvar. );

    /*=========================================
    3. GINI por Variable (Global)
    =========================================*/
    %__gini_variable_m( t1=&t1., t2=&t2., target=&target., var_list=&var_list.,
        param_model_type=&model_type. );

    /*=========================================
    4. GINI Variable por Periodo (CUBO)
    =========================================*/
    %__gini_variable_mensual_m( t1=&t1., t2=&t2., target=&target.,
        var_list=&var_list., time_var=&byvar., param_model_type=&model_type. );

    /*=========================================
    5. Definir formatos condicionales
    =========================================*/
    proc format;
        /* GINI Modelo APP (umbral 0.40) */
        value GiniModeloAPP low -< 0.40='lightred' 0.40 -< 0.50='lightyellow'
            0.50 - high='lightgreen';

        /* GINI Modelo BHV (umbral 0.50) */
        value GiniModeloBHV low -< 0.50='lightred' 0.50 -< 0.60='lightyellow'
            0.60 - high='lightgreen';

        /* GINI Variables (umbral 0.05) */
        value GiniVariable low -< 0.05='lightred' 0.05 -< 0.15='lightyellow'
            0.15 - high='lightgreen';

        /* Delta GINI */
        value DeltaGini low -< -0.05='lightred' -0.05 -< 0.05='lightyellow' 0.05
            - high='lightgreen';

        /* Tendencia texto */
        value $Tendencia 'EMPEORANDO'='lightred' 'ESTABLE'='lightyellow'
            'MEJORANDO'='lightgreen' 'SIN DATOS'='lightgray';

        /* Evaluación texto */
        value $Evaluacion 'BAJO'='lightred' 'ACEPTABLE'='lightyellow'
            'SATISFACTORIO'='lightgreen';
    run;

    /* Seleccionar formato según tipo de modelo */
    %if %upcase(&model_type.)=BHV %then %let fmt_modelo=GiniModeloBHV.;
    %else %let fmt_modelo=GiniModeloAPP.;

    /*=========================================
    6. Generar Excel
    =========================================*/
    ods excel
        file="&&path_troncal_&tr./&_excel_path./tro_&tr._seg_&seg._gini.xlsx"
        options( embedded_titles="yes" embedded_footnotes="yes"
        frozen_headers="yes" autofilter="all" );

    /*--- HOJA 1: MODELO ---*/
    ods excel options(sheet_name="MODELO" sheet_interval="none");

    title "GINI del Modelo - Resumen Global";
    title2 "Tipo: &model_type. | Umbral Aceptable: &thres_modelo.";
    footnote "Troncal: &tr. | Segmento: &seg.";

    proc report data=gini_modelo nowd;
        columns Dataset N_Obs N_Default N_No_Default Tasa_Default Gini
            IC_95_Lower IC_95_Upper;

        define Dataset / display "Dataset";
        define N_Obs / display "N Total" format=comma12.;
        define N_Default / display "N Default" format=comma12.;
        define N_No_Default / display "N No Default" format=comma12.;
        define Tasa_Default / display "Tasa Default" format=percent8.2;
        define Gini / display "GINI" format=8.4
            style(column)=[backgroundcolor=&fmt_modelo.];
        define IC_95_Lower / display "IC 95% Inf" format=8.4;
        define IC_95_Upper / display "IC 95% Sup" format=8.4;
    run;
    title2;

    /*--- HOJA 2: MODELO_MENSUAL ---*/
    ods excel options(sheet_name="MODELO_MENSUAL" sheet_interval="now");

    title "GINI del Modelo por Periodo";
    proc report data=cubo_gini_modelo nowd;
        columns Dataset Periodo N N_Default Tasa_Default Gini;

        define Dataset / display "Dataset";
        define Periodo / display "Periodo" format=6.;
        define N / display "N Total" format=comma12.;
        define N_Default / display "N Default" format=comma12.;
        define Tasa_Default / display "Tasa Default" format=percent8.2;
        define Gini / display "GINI" format=8.4
            style(column)=[backgroundcolor=&fmt_modelo.];
    run;

    /*--- HOJA 3: VARIABLES ---*/
    ods excel options(sheet_name="VARIABLES" sheet_interval="now");

    title "GINI por Variable";

    proc report data=gini_variables nowd;
        columns Variable Dataset N_Obs N_Valid Pct_Valid N_Default Gini Ranking;

        define Variable / display "Variable";
        define Dataset / display "Dataset";
        define N_Obs / display "N Total" format=comma12.;
        define N_Valid / display "N Válidos" format=comma12.;
        define Pct_Valid / display "% Válidos" format=percent8.2;
        define N_Default / display "N Default" format=comma12.;
        define Gini / display "GINI" format=8.4
            style(column)=[backgroundcolor=GiniVariable.];
        define Ranking / display "Ranking" format=3.;
    run;

    /*--- HOJA 4: COMPARATIVO ---*/
    ods excel options(sheet_name="COMPARATIVO" sheet_interval="now");

    title "GINI Variables - Comparativo TRAIN vs OOT";
    proc report data=gini_variables_comp nowd;
        columns Variable Gini_Train Gini_OOT Delta_Gini Rank_Train Rank_OOT
            Delta_Rank Estabilidad;

        define Variable / display "Variable";
        define Gini_Train / display "GINI Train" format=8.4
            style(column)=[backgroundcolor=GiniVariable.];
        define Gini_OOT / display "GINI OOT" format=8.4
            style(column)=[backgroundcolor=GiniVariable.];
        define Delta_Gini / display "Delta GINI" format=8.4
            style(column)=[backgroundcolor=DeltaGini.];
        define Rank_Train / display "Rank Train" format=3.;
        define Rank_OOT / display "Rank OOT" format=3.;
        define Delta_Rank / display "Delta Rank" format=4.;
        define Estabilidad / display "Estabilidad"
            style(column)=[backgroundcolor=$Tendencia.];
    run;

    /*--- HOJA 5: RESUMEN ---*/
    ods excel options(sheet_name="RESUMEN" sheet_interval="now");

    title "Resumen GINI Variables";

    proc report data=cubo_gini_resumen nowd;
        columns Variable Dataset N_Periodos First_Period Last_Period Gini_First
            Gini_Last Gini_Promedio Gini_Min Gini_Max Gini_Std Delta_Gini
            Tendencia Evaluacion;

        define Variable / display "Variable";
        define Dataset / display "Dataset";
        define N_Periodos / display "N Periodos" format=3.;
        define First_Period / display "Primer Periodo" format=6.;
        define Last_Period / display "Último Periodo" format=6.;
        define Gini_First / display "GINI Inicial" format=8.4
            style(column)=[backgroundcolor=GiniVariable.];
        define Gini_Last / display "GINI Final" format=8.4
            style(column)=[backgroundcolor=GiniVariable.];
        define Gini_Promedio / display "GINI Promedio" format=8.4
            style(column)=[backgroundcolor=GiniVariable.];
        define Gini_Min / display "GINI Mín" format=8.4;
        define Gini_Max / display "GINI Máx" format=8.4;
        define Gini_Std / display "GINI Std" format=8.4;
        define Delta_Gini / display "Delta GINI" format=8.4
            style(column)=[backgroundcolor=DeltaGini.];
        define Tendencia / display "Tendencia"
            style(column)=[backgroundcolor=$Tendencia.];
        define Evaluacion / display "Evaluación"
            style(column)=[backgroundcolor=$Evaluacion.];
    run;

    /*--- HOJA 6: CUBO_DETALLE ---*/
    ods excel options(sheet_name="CUBO_DETALLE" sheet_interval="now");

    title "CUBO GINI Variables - Detalle por Periodo";

    proc report data=cubo_gini_variables nowd;
        columns Variable Dataset Periodo N N_Valid N_Default Estimate WaldChiSq
            ProbChiSq Gini;

        define Variable / display "Variable";
        define Dataset / display "Dataset";
        define Periodo / display "Periodo" format=6.;
        define N / display "N Total" format=comma10.;
        define N_Valid / display "N Válidos" format=comma10.;
        define N_Default / display "N Default" format=comma10.;
        define Estimate / display "Coeficiente" format=10.4;
        define WaldChiSq / display "Wald Chi2" format=10.2;
        define ProbChiSq / display "P-Value" format=8.4;
        define Gini / display "GINI" format=8.4
            style(column)=[backgroundcolor=GiniVariable.];
    run;
    title;
    footnote;
    /*--- HOJA 7: GRAFICOS ---*/
    
    ods excel options(sheet_name="GRAFICOS" sheet_interval="now");
    ods listing gpath="&&path_troncal_&tr./&_img_path.";

    /* Tendencia TRAIN */
    ods graphics / imagename="tro_&tr._seg_&seg._ginitrain" imagefmt=jpeg;
        %__plot_gini_tendencia( data=cubo_gini_modelo, dataset_filter=TRAIN, thres=&thres_modelo. );
    ods graphics / reset;

    /* Tendencia OOT */
    ods graphics / imagename="tro_&tr._seg_&seg._ginioot" imagefmt=jpeg;
        %__plot_gini_tendencia( data=cubo_gini_modelo, dataset_filter=OOT, thres=&thres_modelo. );
    ods graphics / reset;

    /* Comparativo TRAIN vs OOT */
    ods graphics / imagename="tro_&tr._seg_&seg._ginitrainvsoot" imagefmt=jpeg;
        %__plot_gini_comparativo( data=cubo_gini_modelo, thres=&thres_modelo.);
    ods graphics / reset;

    /* Ranking Variables TRAIN */
    ods graphics / imagename="tro_&tr._seg_&seg._ginitopvars" imagefmt=jpeg;
        %__plot_gini_ranking( data=cubo_gini_resumen, dataset_filter=TRAIN, top_n=15, thres=&thres_var. );
    ods graphics / reset;
    
    ods excel close;
    ods graphics off;


    /*=========================================
    8. Limpieza de datasets temporales
    =========================================*/
    proc datasets lib=work nolist nowarn;
        delete gini_modelo cubo_gini_modelo gini_variables gini_variables_comp
            cubo_gini_variables cubo_gini_resumen;
    quit;

%mend __gini_r;
/*--------------------------------------------------------------
Módulo: GINI Verify
Versión: 4.0
Desarrollador: Joseph Chombo
Fecha: 2025-12-14

Descripción: Validaciones de precondiciones antes de ejecutar GINI

Incluye: gini_r.sas (reportería -> metodología)

Macros incluidas:
- verify_gini      : Verificación estándar (llamada desde metod_steps)
- verify_gini_full : Verificación completa con parámetros explícitos
--------------------------------------------------------------*/
%include "&_root_path./Sources/Modulos/m_gini/__gini_r.sas";

/*==============================================================
MACRO PRINCIPAL: VERIFICACION ESTANDAR
==============================================================*/
%macro verify_gini(data1, data2);
    /*
     * Macro de verificación llamada desde metod_steps
     * Usa las variables de macro globales del entorno:
     *   - &_target.      : Variable target
     *   - &xb_param.     : Variable score (XB o PD)
     *   - &vars_num.     : Lista de variables numéricas
     *   - &_var_time.    : Variable temporal
     *   - &tr.           : Troncal
     *   - &seg.          : Segmento
     *   - &_excel_path.  : Ruta de Excel
     *   - &_img_path.    : Ruta de imágenes
     */
    /* Determinar el contexto */
    %if &seg > 0 %then %do;
        %let context=SEGMENTO;
    %end;
    %else %do;
        %let context=ALL_UNIVERSE;
    %end;

    %put NOTE:============================================================;
    %put NOTE: [GINI VERIFY] Iniciando validaciones - Contexto: &context.;
    %put NOTE:============================================================;

    /*=========================================
    1. Verificar existencia de datasets
    =========================================*/
    %local valid n_obs1 n_obs2;
    %let valid=1;

    %if not %sysfunc(exist(&data1.)) %then %do;
        %put ERROR: [GINI VERIFY] Dataset TRAIN no existe: &data1.;
        %let valid=0;
    %end;
    %else %do;
        %let n_obs1=%sysfunc(attrn(%sysfunc(open(&data1.)), nobs));
        %if &n_obs1. <= 0 %then %do;
            %put ERROR: [GINI VERIFY] Dataset TRAIN sin observaciones;
            %let valid=0;
        %end;
        %else %do;
            %put NOTE: [GINI VERIFY] ✓ Dataset TRAIN existe con &n_obs1. obs;
        %end;
    %end;

    %if not %sysfunc(exist(&data2.)) %then %do;
        %put WARNING: [GINI VERIFY] Dataset OOT no existe: &data2.;
        %put NOTE: [GINI VERIFY] Se ejecutará solo con TRAIN;
    %end;
    %else %do;
        %let n_obs2=%sysfunc(attrn(%sysfunc(open(&data2.)), nobs));
        %put NOTE: [GINI VERIFY] ✓ Dataset OOT existe con &n_obs2. obs;
    %end;

    /*=========================================
    2. Verificar variables requeridas
    =========================================*/
    %if %length(&_target.)=0 %then %do;
        %put ERROR: [GINI VERIFY] Variable target no definida;
        %let valid=0;
    %end;
    %else %do;
        %put NOTE: [GINI VERIFY] ✓ Target: &_target.;
    %end;

    %if %length(&xb_param.)=0 %then %do;
        %put ERROR: [GINI VERIFY] Variable score (xb_param) no definida;
        %let valid=0;
    %end;
    %else %do;
        %put NOTE: [GINI VERIFY] ✓ Score: &xb_param.;
    %end;

    %if %length(&vars_num.)=0 %then %do;
        %put WARNING: [GINI VERIFY] Lista de variables numéricas vacía;
        %put NOTE: [GINI VERIFY] Solo se calculará GINI del modelo;
    %end;
    %else %do;
        %let n_vars=%sysfunc(countw(&vars_num., %str( )));
        %put NOTE: [GINI VERIFY] ✓ Variables numéricas: &n_vars.;
    %end;

    %if %length(&_var_time.)=0 %then %do;
        %put WARNING: [GINI VERIFY] Variable temporal no definida;
        %put NOTE: [GINI VERIFY] No se generarán análisis mensuales;
    %end;
    %else %do;
        %put NOTE: [GINI VERIFY] ✓ Variable tiempo: &_var_time.;
    %end;

    /*=========================================
    3. Ejecutar reporte si validaciones pasan
    =========================================*/
    %put NOTE:============================================================;

    %if &valid.=1 %then %do;
        %put NOTE: [GINI VERIFY] ✓ Validaciones pasaron;
        %put NOTE: [GINI VERIFY] Ejecutando generación de reportes...;
        %put NOTE:============================================================;

        %__gini_r( t1=&data1., t2=&data2., target=&_target., score=&xb_param.,
            var_list=&vars_num., byvar=&_var_time., model_type=BHV );
    %end;
    %else %do;
        %put ERROR: [GINI VERIFY] ✗ Validaciones fallaron - Reporte no generado;
        %put NOTE:============================================================;
    %end;

%mend verify_gini;

/*==============================================================
MACRO ALTERNATIVA: VERIFICACION COMPLETA CON PARAMETROS
==============================================================*/
%macro verify_gini_full( train=, /* Dataset TRAIN */ oot=, /* Dataset OOT */
    target=, /* Variable target (0/1) */ score=,
    /* Variable score del modelo (PD o XB) */ var_list=,
    /* Lista de variables predictoras */ time_var=, /* Variable temporal */
    model_type=APP,/* Tipo de modelo: APP o BHV */ excel_path=,
    /* Ruta archivo Excel */ img_path=/* Ruta para gráficos */ );

    %local valid dsid varnum rc n_train n_oot def_train def_oot;
    %let valid=1;

    %put NOTE:============================================================;
    %put NOTE: [GINI VERIFY FULL] Iniciando validaciones de precondiciones;
    %put NOTE:============================================================;

    /*=========================================
    1. Validar existencia de datasets
    =========================================*/
    %if not %sysfunc(exist(&train.)) %then %do;
        %put ERROR: [GINI VERIFY] Dataset TRAIN no existe: &train.;
        %let valid=0;
    %end;
    %else %do;
        %put NOTE: [GINI VERIFY] ✓ Dataset TRAIN existe: &train.;
    %end;

    %if not %sysfunc(exist(&oot.)) %then %do;
        %put WARNING: [GINI VERIFY] Dataset OOT no existe: &oot.;
        %put NOTE: [GINI VERIFY] Se ejecutará solo con TRAIN;
    %end;
    %else %do;
        %put NOTE: [GINI VERIFY] ✓ Dataset OOT existe: &oot.;
    %end;

    /*=========================================
    2. Validar variable TARGET
    =========================================*/
    %if &valid.=1 %then %do;
        %let dsid=%sysfunc(open(&train.));
        %if &dsid. > 0 %then %do;
            %let varnum=%sysfunc(varnum(&dsid., &target.));
            %if &varnum.=0 %then %do;
                %put ERROR: [GINI VERIFY] Variable TARGET no existe en TRAIN:
                    &target.;
                %let valid=0;
            %end;
            %else %do;
                %put NOTE: [GINI VERIFY] ✓ Variable TARGET encontrada: &target.;
            %end;
            %let rc=%sysfunc(close(&dsid.));
        %end;
    %end;

    /*=========================================
    3. Validar variable SCORE
    =========================================*/
    %if &valid.=1 %then %do;
        %let dsid=%sysfunc(open(&train.));
        %if &dsid. > 0 %then %do;
            %let varnum=%sysfunc(varnum(&dsid., &score.));
            %if &varnum.=0 %then %do;
                %put ERROR: [GINI VERIFY] Variable SCORE no existe en TRAIN:
                    &score.;
                %let valid=0;
            %end;
            %else %do;
                %put NOTE: [GINI VERIFY] ✓ Variable SCORE encontrada: &score.;
            %end;
            %let rc=%sysfunc(close(&dsid.));
        %end;
    %end;

    /*=========================================
    4. Validar suficiente data en TRAIN
    =========================================*/
    %if &valid.=1 %then %do;
        proc sql noprint;
            select count(*), sum(&target.) into :n_train trimmed, :def_train
                trimmed from &train.;
        quit;

        %if &n_train. < 100 %then %do;
            %put ERROR: [GINI VERIFY] TRAIN tiene muy pocas observaciones:
                &n_train.;
            %let valid=0;
        %end;
        %else %do;
            %put NOTE: [GINI VERIFY] ✓ TRAIN tiene &n_train. observaciones;
        %end;

        %if &def_train. < 10 %then %do;
            %put ERROR: [GINI VERIFY] TRAIN tiene muy pocos defaults:
                &def_train.;
            %let valid=0;
        %end;
        %else %do;
            %put NOTE: [GINI VERIFY] ✓ TRAIN tiene &def_train. defaults;
        %end;
    %end;

    /*=========================================
    5. Ejecutar reporte si validaciones pasan
    =========================================*/
    %put NOTE:============================================================;

    %if &valid.=1 %then %do;
        %put NOTE: [GINI VERIFY] ✓ Todas las validaciones pasaron;
        %put NOTE: [GINI VERIFY] Iniciando generación de reportes...;
        %put NOTE:============================================================;

        %__gini_r( t1=&train., t2=&oot., target=&target., score=&score.,
            var_list=&var_list., byvar=&time_var., model_type=&model_type. );
    %end;
    %else %do;
        %put ERROR: [GINI VERIFY] ✗ Validaciones fallaron - Reporte no generado;
        %put NOTE:============================================================;
    %end;

%mend verify_gini_full;
