/* =========================================================================
gini_report.sas - Reportes HTML + Excel + JPEG para Gini
========================================================================= */
%macro _gini_report(report_path=, images_path=, file_prefix=, byvar=,
    model_low=, model_high=, var_low=, var_high=, delta_warn=, top_n=10);

    %local _delta_low;
    %let _delta_low=%sysevalf(-1 * &delta_warn.);

    proc format;
        value GiniModelFmt
            low-<&model_low.='lightred'
            &model_low.-<&model_high.='lightyellow'
            &model_high.-high='lightgreen';
        value GiniVarFmt
            low-<&var_low.='lightred'
            &var_low.-<&var_high.='lightyellow'
            &var_high.-high='lightgreen';
        value DeltaFmt
            low-<&_delta_low.='lightred'
            &_delta_low.-<&delta_warn.='lightyellow'
            &delta_warn.-high='lightgreen';
        value $TrendFmt
            'EMPEORANDO'='lightred'
            'DEGRADACION'='lightred'
            'ACEPTABLE'='lightyellow'
            'ESTABLE'='lightyellow'
            'MEJORA'='lightgreen'
            'MEJORANDO'='lightgreen'
            'SATISFACTORIO'='lightgreen'
            'BAJO'='lightred'
            'SIN DATOS'='lightgray'
            'MIN DATOS'='lightgray';
    run;

    ods graphics on;
    ods listing gpath="&images_path.";
    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");
    ods excel file="&report_path./&file_prefix..xlsx"
        options(sheet_name="MODEL_GENERAL" sheet_interval="none"
        embedded_titles="yes" frozen_headers="yes" autofilter="all");

    title "GINI del Modelo - Resumen Global";
    proc print data=casuser._gini_model_general noobs label;
        var Split N_Total N_Default N_No_Default Tasa_Default N_Gini Gini
            IC_95_Lower IC_95_Upper Degradacion Evaluacion;
        label Split="Dataset"
            N_Total="N Total"
            N_Default="N Default"
            N_No_Default="N No Default"
            Tasa_Default="Tasa Default"
            N_Gini="N Gini"
            IC_95_Lower="IC 95% Inf"
            IC_95_Upper="IC 95% Sup"
            Degradacion="Degradacion"
            Evaluacion="Evaluacion";
        format Tasa_Default percent8.2 Gini IC_95_Lower IC_95_Upper 8.4
            Degradacion percent8.2;
    run;
    title;

    ods excel options(sheet_name="MODEL_MONTHLY" sheet_interval="now");
    title "GINI del Modelo por Periodo";
    proc print data=casuser._gini_model_monthly noobs label;
        var Split Periodo N_Total N_Default Tasa_Default N_Gini Gini
            Delta_Gini Tendencia Evaluacion;
        label Split="Dataset"
            Periodo="Periodo"
            N_Total="N Total"
            N_Default="N Default"
            Tasa_Default="Tasa Default"
            N_Gini="N Gini"
            Delta_Gini="Delta Gini"
            Tendencia="Tendencia"
            Evaluacion="Evaluacion";
        format Periodo 6. Tasa_Default percent8.2 Gini Delta_Gini 8.4;
    run;
    title;

    ods excel options(sheet_name="VARS_GENERAL" sheet_interval="now");
    title "GINI por Variable";
    proc print data=casuser._gini_vars_general noobs label;
        var Variable Split N_Total N_Valid Pct_Valid N_Default N_Gini Gini
            Evaluacion;
        label Variable="Variable"
            Split="Dataset"
            N_Total="N Total"
            N_Valid="N Validos"
            Pct_Valid="% Validos"
            N_Default="N Default"
            N_Gini="N Gini"
            Evaluacion="Evaluacion";
        format Pct_Valid percent8.2 Gini 8.4;
    run;
    title;

    ods excel options(sheet_name="VARS_COMPARE" sheet_interval="now");
    title "GINI Variables - Comparativo TRAIN vs OOT";
    proc print data=casuser._gini_vars_compare noobs label;
        var Variable Gini_Train Gini_OOT Delta_Gini Estabilidad;
        label Variable="Variable"
            Gini_Train="GINI Train"
            Gini_OOT="GINI OOT"
            Delta_Gini="Delta GINI"
            Estabilidad="Estabilidad";
        format Gini_Train Gini_OOT Delta_Gini 8.4;
    run;
    title;

    ods excel options(sheet_name="VARS_SUMMARY" sheet_interval="now");
    title "Resumen GINI Variables";
    proc print data=casuser._gini_vars_summary noobs label;
        var Variable Split N_Periodos First_Period Last_Period Gini_First
            Gini_Last Gini_Promedio Gini_Min Gini_Max Gini_Std Delta_Gini
            Tendencia Evaluacion;
        label Variable="Variable"
            Split="Dataset"
            N_Periodos="N Periodos"
            First_Period="Primer Periodo"
            Last_Period="Ultimo Periodo"
            Gini_First="GINI Inicial"
            Gini_Last="GINI Final"
            Gini_Promedio="GINI Promedio"
            Gini_Min="GINI Min"
            Gini_Max="GINI Max"
            Gini_Std="GINI Std"
            Delta_Gini="Delta GINI"
            Tendencia="Tendencia"
            Evaluacion="Evaluacion";
        format First_Period Last_Period 6. Gini_First Gini_Last
            Gini_Promedio Gini_Min Gini_Max Gini_Std Delta_Gini 8.4;
    run;
    title;

    ods excel options(sheet_name="VARS_DETAIL" sheet_interval="now");
    title "Cubo GINI Variables - Detalle por Periodo";
    proc print data=casuser._gini_vars_detail noobs label;
        var Variable Split Periodo N_Total N_Valid N_Default N_Gini Gini
            Evaluacion;
        label Variable="Variable"
            Split="Dataset"
            Periodo="Periodo"
            N_Total="N Total"
            N_Valid="N Validos"
            N_Default="N Default"
            N_Gini="N Gini"
            Evaluacion="Evaluacion";
        format Periodo 6. Gini 8.4;
    run;
    title;

    ods excel options(sheet_name="PLOTS" sheet_interval="now");

    ods graphics / imagename="&file_prefix._mdl_trn" imagefmt=jpeg;
    %_gini_plot_model_trend(data=casuser._gini_model_monthly, split=TRAIN,
        model_low=&model_low., model_high=&model_high.);
    ods graphics / reset=all;

    ods graphics / imagename="&file_prefix._mdl_oot" imagefmt=jpeg;
    %_gini_plot_model_trend(data=casuser._gini_model_monthly, split=OOT,
        model_low=&model_low., model_high=&model_high.);
    ods graphics / reset=all;

    ods graphics / imagename="&file_prefix._mdl_cmp" imagefmt=jpeg;
    %_gini_plot_model_compare(data=casuser._gini_model_monthly,
        model_low=&model_low., model_high=&model_high.);
    ods graphics / reset=all;

    ods graphics / imagename="&file_prefix._rank_trn" imagefmt=jpeg;
    %_gini_plot_var_ranking(data=casuser._gini_vars_summary, split=TRAIN,
        top_n=&top_n., var_low=&var_low., var_high=&var_high.);
    ods graphics / reset=all;

    ods graphics / imagename="&file_prefix._rank_oot" imagefmt=jpeg;
    %_gini_plot_var_ranking(data=casuser._gini_vars_summary, split=OOT,
        top_n=&top_n., var_low=&var_low., var_high=&var_high.);
    ods graphics / reset=all;

    ods graphics / imagename="&file_prefix._vars_trn" imagefmt=jpeg;
    %_gini_plot_var_trends(detail=casuser._gini_vars_detail,
        summary=casuser._gini_vars_summary, split=TRAIN, top_n=&top_n.,
        var_low=&var_low., var_high=&var_high.);
    ods graphics / reset=all;

    ods graphics / imagename="&file_prefix._vars_oot" imagefmt=jpeg;
    %_gini_plot_var_trends(detail=casuser._gini_vars_detail,
        summary=casuser._gini_vars_summary, split=OOT, top_n=&top_n.,
        var_low=&var_low., var_high=&var_high.);
    ods graphics / reset=all;

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

%mend _gini_report;
