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

    proc sql outobs=&top_n.;
        create table work._gini_top_vars as
        select Variable
        from &summary.
        where Split="&split." and not missing(Gini_Promedio)
        order by Gini_Promedio desc;
    quit;

    proc sql noprint;
        create table work._gini_panel_plot as
        select d.Variable, d.Periodo, d.Gini
        from &detail. d
        inner join work._gini_top_vars t
            on d.Variable=t.Variable
        where d.Split="&split." and not missing(d.Gini)
        order by d.Variable, d.Periodo;
    quit;

    %local _panel_n;
    %let _panel_n=0;
    proc sql noprint;
        select count(*) into :_panel_n trimmed from work._gini_panel_plot;
    quit;

    %if &_panel_n. > 0 %then %do;
        proc sgpanel data=work._gini_panel_plot;
            title "Tendencia GINI Variables - &split.";
            panelby Variable / columns=2 novarname;
            series x=Periodo y=Gini / markers
                lineattrs=(thickness=2 color=black);
            refline &var_low. / axis=y
                lineattrs=(color=orange pattern=2 thickness=2);
            refline &var_high. / axis=y
                lineattrs=(color=limegreen pattern=2 thickness=2);
            colaxis label='Periodo' type=discrete;
            rowaxis label='Gini' min=0 max=1;
        run;
        title;
    %end;

    proc datasets library=work nolist nowarn;
        delete _gini_top_vars _gini_panel_plot;
    quit;

%mend _gini_plot_var_trends;
