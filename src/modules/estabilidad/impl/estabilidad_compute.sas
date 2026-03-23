/* =========================================================================
estabilidad_compute.sas - Computo de analisis de estabilidad temporal

Contiene macros de computo que procesan variables y generan graficos
de estabilidad temporal. Llamadas desde estabilidad_report.sas dentro
del contexto ODS.

Macros:
%_estab_var_continuo  - Estabilidad de variable numerica (N, mean, missing)
%_estab_var_discreto  - Estabilidad de variable categorica (distribucion %)
%_estab_variables     - Orquestador: itera variables num + cat

Tablas temporales en casuser (CAS) via PROC FEDSQL.
work se usa como staging para variables categoricas (recodificar missings).
Formato de imagen: JPEG.
========================================================================= */

/* =====================================================================
%_estab_var_continuo - Estabilidad de variable numerica
Calcula N, mean y missing count por periodo via PROC FEDSQL (CAS).
Genera tabla + grafico de barras (N) + linea (promedio).
===================================================================== */
%macro _estab_var_continuo(data=, var=, byvar=);

    /* Agregar por periodo: N, mean, missing via FEDSQL */
    proc fedsql sessref=conn noprint;
        create table casuser._estab_cont {options replace=true} as select
            "%upcase(&var.)" as Variable, Split, &byvar., count(*) as N_Obs,
            sum(case when &var. is not null then 1 else 0 end) as N,
            avg(cast(&var. as double)) as prom, sum(case when &var. is null then
            1 else 0 end) as MISSING from &data. group by Split, &byvar.;
    quit;

    proc cas;
        session conn;
        table.partition /
            table={caslib="casuser", name="_estab_cont",
                orderby={"Variable", "&byvar.", "Split"}, groupby={}},
            casout={caslib="casuser", name="_estab_cont", replace=true};
    quit;

    proc print data=casuser._estab_cont noobs;
        var Split &byvar. N_Obs N prom MISSING;
        title "Estabilidad de la variable - &var. (TRAIN vs OOT)";
    run;

    proc sgplot data=casuser._estab_cont subpixel noautolegend;
        yaxis label="Cantidad de registros (N)";
        y2axis label="Promedio &var.";
        styleattrs datacolors=(lightblue cx4f81bd)
            datacontrastcolors=(black cx1f497d);
        vbar &byvar. / response=N group=Split groupdisplay=cluster nooutline
            barwidth=0.7 transparency=0.15;
        series x=&byvar. y=prom / group=Split markers y2axis
            lineattrs=(thickness=2);
        xaxis type=discrete discreteorder=data label="&byvar."
            valueattrs=(size=8pt);
        keylegend / title="Split";
    run;
    title;

    proc datasets library=casuser nolist nowarn;
        delete _estab_cont;
    quit;

%mend _estab_var_continuo;

/* =====================================================================
%_estab_var_discreto - Estabilidad de variable categorica
Recodifica missings, calcula distribucion % por periodo y categoria.
Usa work como staging (DATA step para recodificar missings no soporta
CAS-to-CAS).
===================================================================== */
%macro _estab_var_discreto(data=, var=, byvar=);

    /* Copiar a work y recodificar missings */
    data work._estab_disc_stg;
        set &data.;
        if missing(&var.) then &var.='MISSING';
    run;

    /* Distribucion por periodo y categoria */
    proc sql noprint;
        create table work._estab_disc_cnt as select Split, &byvar., &var.,
            count(*) as N from work._estab_disc_stg group by Split, &byvar.,
            &var.;
    quit;

    proc sql noprint;
        create table work._estab_disc_pct as select "%upcase(&var.)" as
            Variable length=128, a.Split, a.&byvar., a.&var., a.N, (a.N * 100.0)
            / b.total as Porcentaje from work._estab_disc_cnt a inner join (
            select Split, &byvar., sum(N) as total from work._estab_disc_cnt
            group by Split, &byvar. ) b on a.Split=b.Split and
            a.&byvar.=b.&byvar.;
    quit;

    data casuser._estab_disc_pct;
        set work._estab_disc_pct;
    run;

    proc cas;
        session conn;
        table.partition /
            table={caslib="casuser", name="_estab_disc_pct",
                orderby={"Variable", "&byvar.", "Split", "&var."}, groupby={}},
            casout={caslib="casuser", name="_estab_disc_pct", replace=true};
    quit;

    proc print data=casuser._estab_disc_pct noobs;
        var Split &byvar. &var. N Porcentaje;
        title "Estabilidad de la variable - &var. (TRAIN vs OOT)";
    run;

    proc sgpanel data=casuser._estab_disc_pct;
        panelby Split / columns=2 onepanel novarname;
        vbar &byvar. / response=Porcentaje group=&var. groupdisplay=cluster;
        colaxis type=discrete discreteorder=data display=(nolabel);
        rowaxis max=100 label="Porcentaje (%)";
        keylegend / title="Categoria";
    run;
    title;

    /* Cleanup work staging */
    proc datasets library=work nolist nowarn;
        delete _estab_disc_stg _estab_disc_cnt _estab_disc_pct;
    quit;

    proc datasets library=casuser nolist nowarn;
        delete _estab_disc_pct;
    quit;

%mend _estab_var_discreto;

/* =====================================================================
%_estab_variables - Orquestador: itera variables num + cat
Llama a _estab_var_continuo para numericas y _estab_var_discreto
para categoricas.
===================================================================== */
%macro _estab_variables(data=, byvar=, vars_num=, vars_cat=);

    %local c v z v_cat;

    /* Procesar variables numericas */
    %if %length(&vars_num.) > 0 %then %do;
        %let c=1;
        %let v=%scan(&vars_num., &c., %str( ));
        %do %while(%length(&v.) > 0);
            %put NOTE: [estabilidad] Procesando variable numerica: &v.;
            %_estab_var_continuo(data=&data., var=&v., byvar=&byvar.);
            %let c=%eval(&c. + 1);
            %let v=%scan(&vars_num., &c., %str( ));
        %end;
    %end;

    /* Procesar variables categoricas */
    %if %length(&vars_cat.) > 0 %then %do;
        %let z=1;
        %let v_cat=%scan(&vars_cat., &z., %str( ));
        %do %while(%length(&v_cat.) > 0);
            %put NOTE: [estabilidad] Procesando variable categorica: &v_cat.;
            %_estab_var_discreto(data=&data., var=&v_cat., byvar=&byvar.);
            %let z=%eval(&z. + 1);
            %let v_cat=%scan(&vars_cat., &z., %str( ));
        %end;
    %end;

%mend _estab_variables;
