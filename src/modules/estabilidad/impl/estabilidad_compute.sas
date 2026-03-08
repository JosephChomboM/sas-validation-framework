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
            &byvar., count(*) as N_Obs, sum(case when &var. is not null then 1
            else 0 end) as N, avg(cast(&var. as double)) as prom, sum(case when
            &var. is null then 1 else 0 end) as MISSING from &data. group by
            &byvar.;
    quit;

    proc print data=casuser._estab_cont noobs;
        title "Estabilidad de la variable - &var.";
    run;

    proc sgplot data=casuser._estab_cont subpixel noautolegend;
        yaxis label="Cantidad de registros (N)" discreteorder=data;
        y2axis label="Promedio &var.";
        vbar &byvar. / response=N nooutline barwidth=0.4
            fillattrs=(color=lightblue);
        vline &byvar. / response=prom markers markerattrs=(symbol=circlefilled
            color=black) y2axis lineattrs=(color=black);
        xaxis label="&byvar." valueattrs=(size=8pt);
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
        create table work._estab_disc_cnt as select &byvar., &var., count(*) as
            N from work._estab_disc_stg group by &byvar., &var.;
    quit;

    proc sql noprint;
        create table work._estab_disc_pct as select a.&byvar., a.&var., a.N,
            (a.N * 100.0) / b.total as Porcentaje from work._estab_disc_cnt a
            inner join ( select &byvar., sum(N) as total from
            work._estab_disc_cnt group by &byvar. ) b on a.&byvar.=b.&byvar.;
    quit;

    proc print data=work._estab_disc_pct noobs;
        title "Estabilidad de la variable - &var.";
    run;

    proc sgplot data=work._estab_disc_pct;
        vbar &byvar. / response=Porcentaje group=&var. groupdisplay=cluster;
        xaxis display=(nolabel);
        yaxis max=100 label="Porcentaje (%)";
    run;
    title;

    /* Cleanup work staging */
    proc datasets library=work nolist nowarn;
        delete _estab_disc_stg _estab_disc_cnt _estab_disc_pct;
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
