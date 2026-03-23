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
        vbar &byvar. / response=N group=Muestra groupdisplay=cluster nooutline
            barwidth=0.7 transparency=0.15;
        vline &byvar. / response=prom group=Muestra y2axis markers
            lineattrs=(thickness=2);
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
    data work._estab_disc_stg;
        set &data.;
        if missing(&var.) then &var.='MISSING';
    run;

    data casuser._estab_disc_stg;
        set work._estab_disc_stg;
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

    data work._estab_disc_area;
        set casuser._estab_disc_pct;
        by Variable &byvar. Muestra &var.;
        retain _cum_pct 0;
        if first.Muestra or first.&byvar. then _cum_pct=0;
        Lower=_cum_pct;
        Upper=_cum_pct + Porcentaje;
        _cum_pct=Upper;
    run;

    proc sgplot data=work._estab_disc_area(where=(Muestra='TRAIN'))
        noautolegend;
        title "Estabilidad de la variable - &var. (TRAIN)";
        band x=&byvar. lower=Lower upper=Upper / group=&var.;
        xaxis type=discrete discreteorder=data display=(nolabel);
        yaxis max=100 label="Porcentaje (%)";
        keylegend / title="Categoria";
    run;

    proc sgplot data=work._estab_disc_area(where=(Muestra='OOT'))
        noautolegend;
        title "Estabilidad de la variable - &var. (OOT)";
        band x=&byvar. lower=Lower upper=Upper / group=&var.;
        xaxis type=discrete discreteorder=data display=(nolabel);
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
%_estab_variables - Orquestador: itera variables num + cat
Llama a _estab_var_continuo para numericas y _estab_var_discreto
para categoricas.
===================================================================== */
%macro _estab_variables(data=, byvar=, vars_num=, vars_cat=);

    %local _i _v _tipo _nvars c z v v_cat;

    data work._estab_cola_vars;
        length Variable $128 Tipo_Variable $3;
        %let c=1;
        %let v=%scan(&vars_num., &c., %str( ));
        %do %while(%length(&v.) > 0);
            Variable="%upcase(&v.)";
            Tipo_Variable="NUM";
            output;
            %let c=%eval(&c. + 1);
            %let v=%scan(&vars_num., &c., %str( ));
        %end;

        %let z=1;
        %let v_cat=%scan(&vars_cat., &z., %str( ));
        %do %while(%length(&v_cat.) > 0);
            Variable="%upcase(&v_cat.)";
            Tipo_Variable="CAT";
            output;
            %let z=%eval(&z. + 1);
            %let v_cat=%scan(&vars_cat., &z., %str( ));
        %end;
        stop;
    run;

    proc sort data=work._estab_cola_vars nodupkey;
        by Variable;
    run;

    data _null_;
        set work._estab_cola_vars end=_eof;
        call symputx(cats('_estab_var_', _n_), Variable, 'L');
        call symputx(cats('_estab_tipo_', _n_), Tipo_Variable, 'L');
        if _eof then call symputx('_nvars', _n_, 'L');
    run;

    %if %length(%superq(_nvars))=0 %then %let _nvars=0;

    %do _i=1 %to &_nvars.;
        %let _v=&&_estab_var_&_i.;
        %let _tipo=&&_estab_tipo_&_i.;
        %put NOTE: [estabilidad] Procesando variable &_tipo.: &_v.;
        %if &_tipo.=NUM %then %_estab_var_continuo(data=&data., var=&_v.,
            byvar=&byvar.);
        %else %_estab_var_discreto(data=&data., var=&_v., byvar=&byvar.);
    %end;

    proc datasets library=work nolist nowarn;
        delete _estab_cola_vars;
    quit;

%mend _estab_variables;
