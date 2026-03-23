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
            '%upcase(&var.)' as Variable, Muestra, &byvar.,
            count(*) as N_Obs, count(&var.) as N,
            avg(cast(&var. as double)) as prom,
            count(*) - count(&var.) as MISSING from &data.
            group by Muestra, &byvar.;
    quit;

    proc cas;
        session conn;
        table.partition /
            table={caslib="casuser", name="_estab_cont",
                groupby={"Variable"}, orderby={"&byvar.", "Muestra"}},
            casout={caslib="casuser", name="_estab_cont", replace=true};
    quit;

    proc print data=casuser._estab_cont noobs;
        var Muestra &byvar. N_Obs N prom MISSING;
        title "Estabilidad de la variable - &var. (TRAIN y OOT)";
    run;

    proc sgplot data=casuser._estab_cont subpixel noautolegend;
        yaxis label="Cantidad de registros (N)";
        y2axis label="Promedio &var.";
        styleattrs datacolors=(lightblue cx4f81bd)
            datacontrastcolors=(black cx1f497d);
        vbarparm category=&byvar. response=N / group=Muestra
            groupdisplay=cluster nooutline
            barwidth=0.7 transparency=0.15;
        series x=&byvar. y=prom / group=Muestra y2axis markers
            lineattrs=(thickness=1);
        xaxis type=discrete discreteorder=data label="&byvar."
            valueattrs=(size=8pt);
        keylegend / title="Muestra";
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
    data casuser._estab_disc_stg;
        set &data.;
        if missing(&var.) then &var.='MISSING';
    run;

    /* Distribucion por periodo y categoria */
    proc fedsql sessref=conn noprint;
        create table casuser._estab_disc_cnt {options replace=true} as select
            Muestra, &byvar., &var., count(*) as N
            from casuser._estab_disc_stg
            group by Muestra, &byvar., &var.;
    quit;

    proc fedsql sessref=conn noprint;
        create table casuser._estab_disc_pct {options replace=true} as select
            '%upcase(&var.)' as Variable, a.Muestra, a.&byvar., a.&var., a.N,
            (a.N * 100.0) / b.total as Porcentaje
            from casuser._estab_disc_cnt a
            inner join (
                select Muestra, &byvar., sum(N) as total
                from casuser._estab_disc_cnt
                group by Muestra, &byvar.
            ) b
            on a.Muestra=b.Muestra and
            a.&byvar.=b.&byvar.;
    quit;

    proc cas;
        session conn;
        table.partition /
            table={caslib="casuser", name="_estab_disc_pct",
                groupby={"Variable"}, orderby={"&byvar.", "Muestra", "&var."}},
            casout={caslib="casuser", name="_estab_disc_pct", replace=true};
    quit;

    proc print data=casuser._estab_disc_pct noobs;
        var Muestra &byvar. &var. N Porcentaje;
        title "Estabilidad de la variable - &var. (TRAIN y OOT)";
    run;

    data casuser._estab_disc_area;
        set casuser._estab_disc_pct;
        by Variable &byvar. Muestra &var.;
        length Eje_X $32;
        retain _cum_pct 0;
        if first.Muestra or first.&byvar. then _cum_pct=0;
        Lower=_cum_pct;
        Upper=_cum_pct + Porcentaje;
        _cum_pct=Upper;
        Eje_X=cats(put(&byvar., best.-l), '_', Muestra);
    run;

    proc sgplot data=casuser._estab_disc_area noautolegend;
        title "Estabilidad de la variable - &var. (TRAIN y OOT)";
        band x=Eje_X lower=Lower upper=Upper / group=&var.
            transparency=0.20;
        xaxis type=discrete discreteorder=data label="&byvar. / Muestra"
            valueattrs=(size=8pt);
        yaxis max=100 label="Porcentaje (%)";
        keylegend / title="Categoria";
    run;
    title;

    /* Cleanup work staging */
    proc datasets library=work nolist nowarn;
        delete _estab_disc_stg _estab_disc_area;
    quit;

    proc datasets library=casuser nolist nowarn;
        delete _estab_disc_stg _estab_disc_cnt _estab_disc_pct;
    quit;

%mend _estab_var_discreto;

/* =====================================================================
%_estab_variables - Orquestador: itera variables numericas y categoricas
usando exactamente las listas recibidas.
===================================================================== */
%macro _estab_variables(data=, byvar=, vars_num=, vars_cat=);

    %local _i _v;

    %let _i=1;
    %let _v=%scan(&vars_num., &_i., %str( ));
    %do %while(%length(&_v.) > 0);
        %put NOTE: [estabilidad] Procesando variable numerica: &_v.;
        %_estab_var_continuo(data=&data., var=&_v., byvar=&byvar.);
        %let _i=%eval(&_i. + 1);
        %let _v=%scan(&vars_num., &_i., %str( ));
    %end;

    %let _i=1;
    %let _v=%scan(&vars_cat., &_i., %str( ));
    %do %while(%length(&_v.) > 0);
        %put NOTE: [estabilidad] Procesando variable categorica: &_v.;
        %_estab_var_discreto(data=&data., var=&_v., byvar=&byvar.);
        %let _i=%eval(&_i. + 1);
        %let _v=%scan(&vars_cat., &_i., %str( ));
    %end;

%mend _estab_variables;
