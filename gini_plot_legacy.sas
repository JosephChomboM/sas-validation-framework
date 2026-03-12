
/*==============================================================
MACROS DE GRAFICOS
==============================================================*/

/*--------------------------------------------------------------
Macro auxiliar: __nobs
Descripción: Contar observaciones en un dataset
--------------------------------------------------------------*/
%macro __nobs(ds);
    %local dsid nobs;
    %let dsid=%sysfunc(open(&ds.));
    %if &dsid. %then %do;
        %let nobs=%sysfunc(attrn(&dsid., nobs));
        %let dsid=%sysfunc(close(&dsid.));
    %end;
    %else %let nobs=0;
    &nobs. %mend __nobs;

/*--------------------------------------------------------------
Macro: __plot_gini_tendencia
Descripción: Gráfico de barras (N) + línea (GINI) por periodo
--------------------------------------------------------------*/
%macro __plot_gini_tendencia( data=cubo_gini_modelo, dataset_filter=, thres=0.50
    );
    %local thres_sup dsid nobs;
    %let thres_sup=%sysevalf(&thres. + 0.10);

    /* Verificar datos */
    %let dsid=%sysfunc(open(&data.));
    %if &dsid.=0 %then %do;
        %put WARNING: [GINI PLOT] Dataset &data. no existe;
        %return;
    %end;
    %let nobs=%sysfunc(attrn(&dsid., nobs));
    %let dsid=%sysfunc(close(&dsid.));
    %if &nobs.=0 %then %do;
        %put WARNING: [GINI PLOT] Dataset &data. sin observaciones;
        %return;
    %end;

    /* Filtrar y ordenar datos */
    proc sql;
        create table _plot_data as select Periodo, N, Gini from &data. where
            Dataset="&dataset_filter." and Gini is not missing order by Periodo;
    quit;

    %if %__nobs(_plot_data)=0 %then %do;
        %put WARNING: [GINI PLOT] Sin datos para &dataset_filter.;

        proc datasets lib=work nolist nowarn;
            delete _plot_data;
        quit;
        %return;
    %end;

    proc sgplot data=_plot_data;
        title "GINI Modelo - &dataset_filter.";
        vbar Periodo / response=N transparency=0.7 barwidth=0.5 name='bar'
                        fillattrs=(color=gray) 
                        legendlabel='Cuentas' DATALABELFITPOLICY=ROTATE;
        vline Periodo / response=Gini markers 
                        markerattrs=(symbol=circlefilled color=black size=10px)
                        lineattrs=(thickness=0 color=black)
                        name='line' legendlabel='Gini' y2axis;
        refline &thres. / axis=y2 lineattrs=(color=orange pattern=2 thickness=2)
                        labelloc=inside labelattrs=(color=orange) name='acep' legendlabel='Aceptable';
        refline &thres_sup. / axis=y2 lineattrs=(color=limegreen pattern=2 thickness=2) 
                        labelloc=inside labelattrs=(color=limegreen) name='sat' legendlabel='Satisfactorio';
        yaxis grid display=(nolabel) offsetmin=0;
        yaxis label="Cuentas" min=0;
        y2axis grid label="Gini" min=0 max=1;
        xaxis display=all label="Periodo" type=discrete;
        keylegend 'bar' 'line' 'acep' 'sat' / position=bottom noborder;
    run;
    title;

    proc datasets lib=work nolist nowarn;
        delete _plot_data;
    quit;

%mend __plot_gini_tendencia;

/*--------------------------------------------------------------
Macro: __plot_gini_comparativo
Descripción: Gráfico de líneas comparando TRAIN y OOT
--------------------------------------------------------------*/
%macro __plot_gini_comparativo( data=cubo_gini_modelo, thres=0.50 );
    %local thres_sup dsid nobs;
    %let thres_sup=%sysevalf(&thres. + 0.10);

    /* Verificar datos */
    %let dsid=%sysfunc(open(&data.));
    %if &dsid.=0 %then %do;
        %put WARNING: [GINI PLOT] Dataset &data. no existe;
        %return;
    %end;
    %let nobs=%sysfunc(attrn(&dsid., nobs));
    %let dsid=%sysfunc(close(&dsid.));

    /* Filtrar y ordenar */
    proc sql;
        create table _plot_data as select Dataset, Periodo, Gini from &data.
            where Gini is not missing order by Periodo, Dataset;
    quit;

    %if %__nobs(_plot_data)=0 %then %do;
        %put WARNING: [GINI PLOT] Sin datos para comparativo;

        proc datasets lib=work nolist nowarn;
            delete _plot_data;
        quit;
        %return;
    %end;

    proc sgplot data=_plot_data;
        title "GINI Modelo - Comparativo TRAIN vs OOT";
        series x=Periodo y=Gini / group=Dataset markers
            markerattrs=(symbol=circlefilled size=10px) lineattrs=(thickness=2)
            name='series';
        refline &thres. / axis=y lineattrs=(color=orange pattern=2 thickness=2)
                        labelloc=inside labelattrs=(color=orange) name='acep'
                        legendlabel='Aceptable';
        refline &thres_sup. / axis=y lineattrs=(color=limegreen pattern=2 thickness=2)
                            labelloc=inside labelattrs=(color=limegreen)
                            name='sat' legendlabel='Satisfactorio';
        yaxis grid label="Gini" min=0 max=1;
        xaxis display=all label="Periodo" type=discrete;
        keylegend 'series' 'acep' 'sat' / position=bottom noborder;
    run;
    title;

    proc datasets lib=work nolist nowarn;
        delete _plot_data;
    quit;

%mend __plot_gini_comparativo;

/*--------------------------------------------------------------
Macro: __plot_gini_ranking
Descripción: Gráfico de barras horizontales con ranking de variables
--------------------------------------------------------------*/
%macro __plot_gini_ranking( data=cubo_gini_resumen, dataset_filter=TRAIN,
    top_n=15, thres=0.05 );
    %local thres_sup;
    %let thres_sup=%sysevalf(&thres. + 0.10);

    /* Verificar datos */
    %if %__nobs(&data.)=0 %then %do;
        %put WARNING: [GINI PLOT] Dataset &data. sin observaciones;
        %return;
    %end;

    /* Calcular GINI promedio por variable y limitar a top_n */
    proc sql outobs=&top_n.;
        create table _plot_ranking as 
        select
            Variable, 
            Gini_Promedio as Gini format=8.4, 
            case 
                when Gini_Promedio >= &thres_sup. then 'Satisfactorio' 
                when Gini_Promedio >= &thres. then 'Aceptable'
                else 'No aceptable' 
            end as Evaluacion length=15 
        from &data.
        where Dataset="&dataset_filter." and Gini_Promedio is not missing
        order by Gini_Promedio descending;
    quit;

    %if %__nobs(_plot_ranking)=0 %then %do;
        %put WARNING: [GINI PLOT] Sin datos para ranking &dataset_filter.;

        proc datasets lib=work nolist nowarn;
            delete _plot_ranking;
        quit;
        %return;
    %end;

    proc sgplot data=_plot_ranking;
        title "Ranking GINI Variables - &dataset_filter.";
        title2 "Top &top_n. variables";

        hbar Variable / response=Gini categoryorder=respdesc
            fillattrs=(color=CX4472C4) datalabel datalabelattrs=(size=8pt);

        refline &thres. / axis=x lineattrs=(color=orange pattern=2 thickness=2)
            labelloc=inside name='acep' legendlabel="Aceptable (&thres.)";

        refline &thres_sup. / axis=x lineattrs=(color=limegreen pattern=2
            thickness=2) labelloc=inside name='sat' legendlabel="Satisfactorio (&thres_sup.)";

        xaxis label="Coeficiente Gini" min=0 max=1 grid;
        yaxis label="Variable" fitpolicy=thin;
        keylegend 'acep' 'sat' / position=bottom noborder;
    run; 
    title;
    title2;

    proc datasets lib=work nolist nowarn;
        delete _plot_ranking;
    quit;

%mend __plot_gini_ranking;

/*--------------------------------------------------------------
Macro: __plot_gini_variables
Descripción: Genera un gráfico por cada variable en el cubo
--------------------------------------------------------------*/
%macro __plot_gini_variables( data=cubo_gini_variables, dataset_filter=TRAIN,
    top_n=10, thres=0.05 );
    %local thres_sup n_vars i var_plot var_list;
    %let thres_sup=%sysevalf(&thres. + 0.10);

    /* Verificar datos */
    %if %__nobs(&data.)=0 %then %do;
        %put WARNING: [GINI PLOT] Dataset &data. sin observaciones;
        %return;
    %end;

    /* Obtener lista de variables ordenadas por GINI promedio descendente */
    proc sql noprint;
        select distinct Variable into :var_list separated by '|' from ( select
            Variable, mean(Gini) as Gini_Mean from &data. where Dataset=
            "&dataset_filter." and Gini is not missing group by Variable order
            by Gini_Mean descending );
        %let n_vars=&sqlobs.;
    quit;

    %if &n_vars.=0 %then %do;
        %put WARNING: [GINI PLOT] Sin variables para &dataset_filter.;
        %return;
    %end;

    /* Aplicar tope si se especifica */
    %if &top_n. > 0 and &n_vars. > &top_n. %then %do;
        %let n_vars=&top_n.;
    %end;

    %put NOTE: [GINI PLOT] Generando graficos para &n_vars. variables
        (&dataset_filter.);

    /* Generar gráfico por cada variable */
    %do i=1 %to &n_vars.;
        %let var_plot=%scan(&var_list., &i., |);

        /* Filtrar datos de la variable */
        proc sql;
            create table _plot_var_data as select Periodo, N, Gini from &data.
                where Variable="&var_plot." and Dataset="&dataset_filter." and
                Gini is not missing order by Periodo;
        quit;

        %if %__nobs(_plot_var_data) > 0 %then %do;
            proc sgplot data=_plot_var_data;
                title "GINI Variable: &var_plot. - &dataset_filter.";
                vbar Periodo / response=N transparency=0.7 barwidth=0.5
                    name='bar' fillattrs=(color=gray) legendlabel='Cuentas'
                    DATALABELFITPOLICY=ROTATE;
                vline Periodo / response=Gini markers
                    markerattrs=(symbol=circlefilled color=black size=10px)
                    lineattrs=(thickness=0 color=black) name='line'
                    legendlabel='Gini' y2axis;
                refline &thres. / axis=y2 lineattrs=(color=orange pattern=2
                    thickness=2) label="Aceptable" labelattrs=(color=orange)
                    name='acep' legendlabel='Aceptable';
                refline &thres_sup. / axis=y2 lineattrs=(color=limegreen
                    pattern=2 thickness=2) label="Satisfactorio"
                    labelattrs=(color=limegreen) name='sat'
                    legendlabel='Satisfactorio';
                yaxis grid display=(nolabel) offsetmin=0;
                y2axis grid label="Gini" min=0 max=1;
                xaxis display=all label="Periodo";
                keylegend 'bar' 'line' 'acep' 'sat' / position=bottom noborder;
            run;
            title;
        %end;
    %end;

    proc datasets lib=work nolist nowarn;
        delete _plot_var_data;
    quit;

%mend __plot_gini_variables;
