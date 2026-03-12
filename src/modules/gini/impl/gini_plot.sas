/* =========================================================================
gini_plot.sas - Graficos para Gini del modelo y variables
========================================================================= */

%macro _gini_plot_model_trend(data=, split=, model_low=, model_high=);
    proc sgplot data=&data.(where=(Split="&split." and not missing(Gini)));
        title "GINI Modelo - &split.";
        vbar Periodo / response=N_Total transparency=0.7 barwidth=0.5
            fillattrs=(color=gray) name='bar' legendlabel='Cuentas';
        vline Periodo / response=Gini markers
            markerattrs=(symbol=circlefilled color=black size=10px)
            lineattrs=(thickness=0 color=black)
            y2axis name='line' legendlabel='Gini';
        refline &model_low. / axis=y2
            lineattrs=(color=orange pattern=2 thickness=2)
            name='acep' legendlabel='Aceptable';
        refline &model_high. / axis=y2
            lineattrs=(color=limegreen pattern=2 thickness=2)
            name='sat' legendlabel='Satisfactorio';
        yaxis label='Cuentas' min=0;
        y2axis label='Gini' min=0 max=1;
        xaxis label='Periodo' type=discrete;
        keylegend 'bar' 'line' 'acep' 'sat' / position=bottom noborder;
    run;
    title;
%mend _gini_plot_model_trend;

%macro _gini_plot_model_compare(data=, model_low=, model_high=);
    proc sgplot data=&data.(where=(not missing(Gini)));
        title "GINI Modelo - Comparativo TRAIN vs OOT";
        series x=Periodo y=Gini / group=Split markers
            markerattrs=(symbol=circlefilled size=10px)
            lineattrs=(thickness=2) name='series';
        refline &model_low. / axis=y
            lineattrs=(color=orange pattern=2 thickness=2)
            name='acep' legendlabel='Aceptable';
        refline &model_high. / axis=y
            lineattrs=(color=limegreen pattern=2 thickness=2)
            name='sat' legendlabel='Satisfactorio';
        yaxis label='Gini' min=0 max=1;
        xaxis label='Periodo' type=discrete;
        keylegend 'series' 'acep' 'sat' / position=bottom noborder;
    run;
    title;
%mend _gini_plot_model_compare;

%macro _gini_plot_var_ranking(data=, split=TRAIN, top_n=10, var_low=,
    var_high=);
    proc sql outobs=&top_n.;
        create table work._gini_rank_plot as
        select Variable, Gini_Promedio as Gini format=8.4
        from &data.
        where Split="&split." and not missing(Gini_Promedio)
        order by Gini_Promedio desc;
    quit;

    %local _rank_n;
    %let _rank_n=0;
    proc sql noprint;
        select count(*) into :_rank_n trimmed from work._gini_rank_plot;
    quit;

    %if &_rank_n. > 0 %then %do;
        proc sgplot data=work._gini_rank_plot;
            title "Ranking GINI Variables - &split.";
            title2 "Top &top_n. variables";
            hbar Variable / response=Gini categoryorder=respdesc
                fillattrs=(color=CX4472C4) datalabel;
            refline &var_low. / axis=x
                lineattrs=(color=orange pattern=2 thickness=2)
                name='acep' legendlabel='Aceptable';
            refline &var_high. / axis=x
                lineattrs=(color=limegreen pattern=2 thickness=2)
                name='sat' legendlabel='Satisfactorio';
            xaxis label='Gini' min=0 max=1 grid;
            yaxis label='Variable';
            keylegend 'acep' 'sat' / position=bottom noborder;
        run;
        title;
        title2;
    %end;

    proc datasets library=work nolist nowarn;
        delete _gini_rank_plot;
    quit;
%mend _gini_plot_var_ranking;

%macro _gini_plot_var_trends(detail=, summary=, split=TRAIN, top_n=10,
    var_low=, var_high=);

    %local _nvars _i _var;

    proc sql noprint outobs=&top_n.;
        select Variable into :_gini_plot_v1-
        from &summary.
        where Split="&split." and not missing(Gini_Promedio)
        order by Gini_Promedio desc;
        %let _nvars=&sqlobs.;
    quit;

    %do _i=1 %to &_nvars.;
        %let _var=&&_gini_plot_v&_i.;
        proc sgplot data=&detail.(where=(Split="&split." and Variable="&_var." and not missing(Gini)));
            title "GINI Variable - &split.: &_var.";
            vbar Periodo / response=N_Total transparency=0.7 barwidth=0.5
                fillattrs=(color=gray) name='bar' legendlabel='Cuentas';
            vline Periodo / response=Gini markers
                markerattrs=(symbol=circlefilled color=black size=10px)
                lineattrs=(thickness=0 color=black)
                y2axis name='line' legendlabel='Gini';
            refline &var_low. / axis=y2
                lineattrs=(color=orange pattern=2 thickness=2)
                name='acep' legendlabel='Aceptable';
            refline &var_high. / axis=y2
                lineattrs=(color=limegreen pattern=2 thickness=2)
                name='sat' legendlabel='Satisfactorio';
            yaxis label='Cuentas' min=0;
            y2axis label='Gini' min=0 max=1;
            xaxis label='Periodo' type=discrete;
            keylegend 'bar' 'line' 'acep' 'sat' / position=bottom noborder;
        run;
        title;
    %end;

%mend _gini_plot_var_trends;
