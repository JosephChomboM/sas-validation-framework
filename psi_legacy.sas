/*--------------------------------------------------------------
Version: 2.3
Desarrollador: Joseph Chombo
Fecha Release: 02/12/2025
Módulo: PSI - Population Stability Index
Objetivo: Detectar drift en distribución de variables para PD scoring
Optimización: Usa PROC SQL en lugar de código dinámico
--------------------------------------------------------------*/

/*--------------------------------------------------------------
Macro interna: Cálculo del PSI entre dos datasets
Método optimizado usando PROC SQL y PROC RANK directo
Output: Macro variable &psi_valor
--------------------------------------------------------------*/
%macro _psi_calc(dev=, oot=, var=, n_buckets=10, flg_continue=1);

    %local rnd n_cortes i;
    %let rnd=%sysfunc(int(%sysfunc(ranuni(0))*100000));

    %global psi_valor;
    %let psi_valor=0;

    /*----- Variables continuas: crear buckets con PROC RANK -----*/
    %if &flg_continue.=1 %then %do;

        /* Calcular percentiles en DEV para definir cortes */
        proc rank data=&dev.(keep=&var.) out=_rank_dev_&rnd. groups=&n_buckets.;
            var &var.;
            ranks bucket;
        run;

        /* Obtener puntos de corte (max de cada bucket) */
        proc sql noprint;
            create table _cortes_&rnd. as select bucket, max(&var.) as corte
                from _rank_dev_&rnd. where bucket is not missing group by bucket
                order by bucket;

            select count(*) into :n_cortes trimmed from _cortes_&rnd.;
        quit;

        /* Verificar que hay cortes */
        %if &n_cortes. > 0 %then %do;
            proc sql noprint;
                select corte into :corte1 - :corte&n_cortes. from _cortes_&rnd.;
            quit;

            /* Asignar buckets a DEV usando los cortes */
            data _dev_bucket_&rnd.;
                set &dev.(keep=&var.);
                if missing(&var.) then bucket=0;
                %do i=1 %to &n_cortes.;
                    %if &i.=1 %then %do;
                        else if &var. <= &&corte&i. then bucket=&i.;
                    %end;
                    %else %if &i.=&n_cortes. %then %do;
                        else bucket=&i.;
                    %end;
                    %else %do;
                        else if &var. <= &&corte&i. then bucket=&i.;
                    %end;
                %end;
            run;

            /* Asignar buckets a OOT usando los mismos cortes */
            data _oot_bucket_&rnd.;
                set &oot.(keep=&var.);
                if missing(&var.) then bucket=0;
                %do i=1 %to &n_cortes.;
                    %if &i.=1 %then %do;
                        else if &var. <= &&corte&i. then bucket=&i.;
                    %end;
                    %else %if &i.=&n_cortes. %then %do;
                        else bucket=&i.;
                    %end;
                    %else %do;
                        else if &var. <= &&corte&i. then bucket=&i.;
                    %end;
                %end;
            run;
        %end;
        %else %do;
            /* Sin cortes válidos - usar un solo bucket */
            data _dev_bucket_&rnd.;
                set &dev.(keep=&var.);
                bucket=1;
            run;

            data _oot_bucket_&rnd.;
                set &oot.(keep=&var.);
                bucket=1;
            run;
        %end;

    %end;
    /*----- Variables categóricas: usar valores como bucket -----*/
    %else %do;

        data _dev_bucket_&rnd.;
            set &dev.(keep=&var.);
            bucket=&var.;
        run;

        data _oot_bucket_&rnd.;
            set &oot.(keep=&var.);
            bucket=&var.;
        run;

    %end;

    /*----- Calcular frecuencias y PSI con SQL -----*/
    proc sql noprint;
        /* Frecuencias DEV */
        create table _freq_dev_&rnd. as select bucket, count(*) as n_dev from
            _dev_bucket_&rnd. group by bucket;

        /* Frecuencias OOT */
        create table _freq_oot_&rnd. as select bucket, count(*) as n_oot from
            _oot_bucket_&rnd. group by bucket;

        /* Totales */
        select coalesce(sum(n_dev), 0) into :tot_dev trimmed from
            _freq_dev_&rnd.;
        select coalesce(sum(n_oot), 0) into :tot_oot trimmed from
            _freq_oot_&rnd.;
    quit;

    /* Validar que hay datos */
    %if &tot_dev. > 0 and &tot_oot. > 0 %then %do;
        proc sql noprint;
            /* Unir y calcular PSI */
            create table _psi_calc_&rnd. as select coalesce(a.bucket, b.bucket)
                as bucket, coalesce(a.n_dev, 0) as n_dev, coalesce(b.n_oot, 0)
                as n_oot,
                /* Porcentajes con suavizado Laplace para evitar log(0) */
                (coalesce(a.n_dev, 0) + 0.5) / (&tot_dev. + 0.5 * &n_buckets.)
                as pct_dev, (coalesce(b.n_oot, 0) + 0.5) / (&tot_oot. + 0.5 *
                &n_buckets.) as pct_oot from _freq_dev_&rnd. a full join
                _freq_oot_&rnd. b on a.bucket=b.bucket;

            /* Calcular PSI total */
            select sum((pct_oot - pct_dev) * log(pct_oot / pct_dev)) into
                :psi_valor trimmed from _psi_calc_&rnd.;
        quit;
    %end;

    %if %length(&psi_valor.)=0 or &psi_valor.=. %then %let psi_valor=0;

    /* Limpiar temporales */
    proc datasets lib=work nolist nowarn;
        delete _rank_dev_&rnd. _cortes_&rnd. _dev_bucket_&rnd. _oot_bucket_&rnd.
            _freq_dev_&rnd. _freq_oot_&rnd. _psi_calc_&rnd.;
    quit;

%mend _psi_calc;

/*--------------------------------------------------------------
Macro principal: PSI por Variable y Mes
Genera CUBO con estructura: Variable | &byvar | PSI | Tipo
--------------------------------------------------------------*/
%macro __psi_variables( t1=, t2=, byvar=, lista_var=, lista_var_cat=, groups=10,
    mensual=1, with_oot=0, where_oot=. );
    %local n v m c z v_aux es_categorica num_valores;

    /*=========================================
    1. Preparar lista de variables
    Categóricas se marcan con # al final
    =========================================*/
    %let z=1;
    %let v_cat=%scan(&lista_var_cat., &z., %str( ));

    %do %while(%length(&v_cat.) > 0);
        %let lista_var=&lista_var. &v_cat.#;
        %let z=%eval(&z. + 1);
        %let v_cat=%scan(&lista_var_cat., &z., %str( ));
    %end;

    /*=========================================
    2. Preparar datasets DEV y OOT
    =========================================*/
    %if (&with_oot.=1) and (%length(&byvar.) > 0) and (&byvar. ne .) %then %do;
        data psi_dev;
            set &t1.;
            where &byvar. < &where_oot.;
        run;

        data psi_oot;
            set &t1.;
            where &byvar. >= &where_oot.;
        run;
    %end;
    %else %do;
        data psi_dev;
            set &t1.;
        run;

        data psi_oot;
            set &t2.;
        run;
    %end;

    /*=========================================
    3. Obtener lista de meses en OOT
    =========================================*/
    %let meses_oot= ;
    %let n_meses=0;

    %if %length(&byvar.) > 0 and &byvar. ne . %then %do;
        proc sql noprint;
            select distinct &byvar. into :meses_oot separated by ' ' from
                psi_oot order by &byvar.;

            select count(distinct &byvar.) into :n_meses trimmed from psi_oot;
        quit;
    %end;

    %put NOTE: Meses en OOT: &meses_oot.;
    %put NOTE: Total meses: &n_meses.;

    /*=========================================
    4. Inicializar CUBO PSI con todas las columnas
    =========================================*/
    proc sql;
        create table cubo_psi ( Variable char(45), &byvar num, PSI num
            format=10.6, Tipo char(15) );
    quit;

    /*=========================================
    5. Calcular PSI por variable y mes
    =========================================*/
    %let n=1;
    %let v=%scan(&lista_var., &n., %str( ));

    %do %while(%length(&v.) > 0);

        /* Determinar si es categórica */
        %let es_categorica=0;
        %let v_aux=&v.;

        %if %index(&v., %str(#)) > 0 %then %do;
            %let v_aux=%substr(&v., 1, %eval(%length(&v.) - 1));
            %let es_categorica=1;
        %end;
        %else %do;
            proc sql noprint;
                select count(distinct &v_aux.) into :num_valores trimmed from
                    psi_oot;
            quit;
            %if &num_valores. <= 10 %then %let es_categorica=1;
        %end;

        %put Procesando variable &v_aux. (categorica=&es_categorica.);

        /*----- PSI Mensual -----*/
        %if &mensual.=1 and &n_meses. > 0 %then %do;
            %let m=1;
            %let c=%scan(&meses_oot., &m., %str( ));

            %do %while(%length(&c.) > 0);

                data psi_oot_mes;
                    set psi_oot;
                    where &byvar.=&c.;
                run;

                %_psi_calc( dev=psi_dev, oot=psi_oot_mes, var=&v_aux.,
                    n_buckets=&groups., flg_continue=%eval(1 - &es_categorica.)
                    );

                /* Agregar al cubo */
                proc sql;
                    insert into cubo_psi (Variable, &byvar, PSI, Tipo) values
                        ("&v_aux.", &c., &psi_valor., "Mensual");
                quit;

                %let m=%eval(&m. + 1);
                %let c=%scan(&meses_oot., &m., %str( ));
            %end;
        %end;

        /*----- PSI Total (DEV vs OOT completo) -----*/
        %_psi_calc( dev=psi_dev, oot=psi_oot, var=&v_aux., var=&v_aux.,
            n_buckets=&groups., flg_continue=%eval(1 - &es_categorica.) );

        proc sql;
            insert into cubo_psi (Variable, &byvar, PSI, Tipo) values
                ("&v_aux.", 999999, &psi_valor., "Total");
        quit;

        %let n=%eval(&n. + 1);
        %let v=%scan(&lista_var., &n., %str( ));
    %end;

    /*=========================================
    6. Crear CUBO formato wide (Variable × Mes)
    =========================================*/
    proc sort data=cubo_psi;
        by Variable &byvar;
    run;

    /* Solo crear wide si hay datos mensuales */
    %let hay_mensual=0;

    proc sql noprint;
        select count(*) into :hay_mensual trimmed from cubo_psi where
            Tipo="Mensual";
    quit;

    %if &hay_mensual. > 0 %then %do;
        proc transpose data=cubo_psi(where=(Tipo="Mensual"))
            out=cubo_psi_wide(drop=_NAME_) prefix=mes_;
            by Variable;
            id &byvar;
            var PSI;
        run;

        /* Usar tabla intermedia para evitar recursive reference */
        proc sql;
            create table _cubo_psi_wide_tmp as select a.*, b.PSI as PSI_Total
                format=10.6 from cubo_psi_wide a left join
                cubo_psi(where=(Tipo="Total")) b on a.Variable=b.Variable;
        quit;

        proc datasets lib=work nolist nowarn;
            delete cubo_psi_wide;
            change _cubo_psi_wide_tmp=cubo_psi_wide;
        quit;
    %end;
    %else %do;
        /* Sin datos mensuales - crear wide solo con Total */
        proc sql;
            create table cubo_psi_wide as select Variable, PSI as PSI_Total
                format=10.6 from cubo_psi where Tipo="Total";
        quit;
    %end;

    /*=========================================
    7. Resumen con estadísticas y alertas
    =========================================*/
    proc sql;
        create table psi_resumen as select Variable, max(case when Tipo="Total"
            then PSI else . end) as PSI_Total format=10.6, min(case when
            Tipo="Mensual" then PSI else . end) as PSI_Min format=10.6, max(case
            when Tipo="Mensual" then PSI else . end) as PSI_Max format=10.6,
            mean(case when Tipo="Mensual" then PSI else . end) as PSI_Mean
            format=10.6, std(case when Tipo="Mensual" then PSI else . end) as
            PSI_Std format=10.6, sum(case when Tipo="Mensual" and PSI < 0.10
            then 1 else 0 end) as Meses_Verde, sum(case when Tipo="Mensual" and
            PSI >= 0.10 and PSI < 0.25 then 1 else 0 end) as Meses_Amarillo,
            sum(case when Tipo="Mensual" and PSI >= 0.25 then 1 else 0 end) as
            Meses_Rojo, sum(case when Tipo="Mensual" then 1 else 0 end) as
            Total_Meses, max(case when Tipo="Mensual" then &byvar else . end) as
            Ultimo_Mes, min(case when Tipo="Mensual" then &byvar else . end) as
            Primer_Mes from cubo_psi group by Variable;
    quit;

    /* Agregar tendencia y alertas en paso separado */
    proc sql;
        create table _psi_resumen_tmp as select a.*, b.PSI as PSI_Primer_Mes
            format=10.6, c.PSI as PSI_Ultimo_Mes format=10.6, coalesce(c.PSI, 0)
            - coalesce(b.PSI, 0) as Tendencia format=10.6, case when a.PSI_Total
            < 0.10 then 'VERDE' when a.PSI_Total < 0.25 then 'AMARILLO' else
            'ROJO' end as Semaforo_Total length=10, case when coalesce(c.PSI, 0)
            - coalesce(b.PSI, 0) > 0.05 then 'EMPEORANDO' when coalesce(c.PSI,
            0) - coalesce(b.PSI, 0) < -0.05 then 'MEJORANDO' else 'ESTABLE' end
            as Alerta_Tendencia length=15, case when a.Total_Meses > 0 then
            a.Meses_Rojo / a.Total_Meses else 0 end as Pct_Meses_Rojo
            format=percent8.1 from psi_resumen a left join cubo_psi b on
            a.Variable=b.Variable and a.Primer_Mes=b.&byvar and b.Tipo="Mensual"
            left join cubo_psi c on a.Variable=c.Variable and a.Ultimo_Mes=
            c.&byvar and c.Tipo="Mensual";
    quit;

    proc datasets lib=work nolist nowarn;
        delete psi_resumen;
        change _psi_resumen_tmp=psi_resumen;
    quit;

    /*=========================================
    8. Limpiar temporales
    =========================================*/
    proc datasets lib=work nolist nowarn;
        delete psi_dev psi_oot psi_oot_mes;
    quit;

%mend __psi_variables;

/*--------------------------------------------------------------
Macro: Gráfico de tendencia temporal del PSI
--------------------------------------------------------------*/
%macro __plot_psi_tendencia(data=cubo_psi, byvar=);

    %local var_list n_vars i var_name;

    proc sql noprint;
        select distinct Variable into :var_list separated by '|' from &data.
            where Tipo="Mensual";

        select count(distinct Variable) into :n_vars trimmed from &data. where
            Tipo="Mensual";
    quit;

    %if &n_vars.=0 %then %do;
        %put NOTE: No hay datos mensuales para graficar tendencia.;
        %return;
    %end;

    %do i=1 %to &n_vars.;
        %let var_name=%scan(&var_list., &i., |);

        title "PSI Temporal: &var_name.";

        proc sgplot data=&data.(where=(Variable="&var_name." and
            Tipo="Mensual"));
            band x=&byvar lower=0 upper=0.10 / fillattrs=(color=lightgreen
                transparency=0.7) legendlabel="Estable (<0.10)";
            band x=&byvar lower=0.10 upper=0.25 / fillattrs=(color=yellow
                transparency=0.7) legendlabel="Alerta (0.10-0.25)";
            band x=&byvar lower=0.25 upper=0.3 / fillattrs=(color=lightcoral
                transparency=0.7) legendlabel="Crítico (>0.25)";

            series x=&byvar y=PSI / lineattrs=(color=navy thickness=2) markers
                markerattrs=(symbol=circlefilled color=navy size=10);

            refline 0.10 / axis=y lineattrs=(color=orange pattern=dash
                thickness=1);
            refline 0.25 / axis=y lineattrs=(color=red pattern=dash
                thickness=1);

            xaxis label="&byvar" valueattrs=(size=8) type=discrete;
            yaxis label="PSI" min=0 valueattrs=(size=8);
        run;

        title;
    %end;

%mend __plot_psi_tendencia;

/*--------------------------------------------------------------
Módulo: PSI Report
Output: Excel con múltiples hojas
--------------------------------------------------------------*/
%include "&_root_path/Sources/Modulos/m_psi/psi_macro.sas";

%macro __psi_report( t1=, t2=, byvar=, lista_var=, lista_var_cat=);

    /* Ejecutar cálculo de PSI */
    %__psi_variables( t1=&t1., t2=&t2., byvar=&byvar., lista_var=&lista_var.,
        lista_var_cat=&lista_var_cat. );

    proc format;
        value PsiSignif -0.0-<0.1="lightgreen" 0.1-<0.25="yellow" 0.25-<9999=
            "red" ;
    run;
    /* Crear Excel */
    ods excel
        file="&&path_troncal_&tr./&_excel_path./tro_&tr._seg_&seg._PSI.xlsx"
        options(embedded_titles="yes" embedded_footnotes="yes");

    /*--- HOJA 1: CUBO_PSI ---*/
    ods excel options(sheet_name="PSI" sheet_interval="none");
    title "CUBO PSI: Detalle por Variable y Periodo";
    footnote
        "Tipo: Mensual = PSI de ese mes vs TRAIN | Total = PSI OOT completo vs TRAIN";

    proc print data=cubo_psi noobs label style(column)={backgroundcolor=
        PsiSignif.};
        var Variable &byvar. Tipo PSI;
    run;
    title;
    footnote;

    /*--- HOJA 2: CUBO_WIDE ---*/
    ods excel options(sheet_name="PSI_CUBO" sheet_interval="now");
    title "CUBO PSI: (Variable x Mes)";

    proc print data=cubo_psi_wide noobs style(column)={backgroundcolor=
        PsiSignif.};
    run;
    title;

    /*--- HOJA 3: RESUMEN ---*/
    ods excel options(sheet_name="RESUMEN" sheet_interval="now");
    title "Resumen de Estabilidad PSI";

    proc print data=psi_resumen noobs;
        var Variable;
        var PSI_Total / style(data)={backgroundcolor=PsiSignif.};
        var PSI_Min PSI_Max PSI_Mean PSI_Std Meses_Verde Meses_Amarillo
            Meses_Rojo Total_Meses Pct_Meses_Rojo;
        var PSI_Primer_Mes / style(data)={backgroundcolor=PsiSignif.};
        var PSI_Ultimo_Mes / style(data)={backgroundcolor=PsiSignif.};
        var Tendencia Alerta_Tendencia;
    run;
    title;

    /*--- HOJA 4: GRAFICOS ---*/
    ods listing gpath="&&path_troncal_&tr/&_img_path";
    ods graphics / imagename="tro_&tr._seg_&seg._psi_" imagefmt=jpeg;
    ods excel options(sheet_name="GRAFICOS" sheet_interval="now");

    %__plot_psi_tendencia(data=cubo_psi, byvar=&byvar.);
    ods excel close;
    ods graphics / reset;
    ods graphics off;

    %put NOTE:==================================================;
    %put NOTE: PSI Report Generado Exitosamente;
    %put NOTE:==================================================;

    /* Limpiar */
    proc datasets lib=work nolist nowarn;
        delete cubo_psi cubo_psi_wide psi_resumen;
    quit;

%mend __psi_report;

/*--------------------------------------------------------------
Módulo: PSI - Verificación de Precondiciones
Valida: Datasets, variable tiempo, variables numéricas/categóricas
--------------------------------------------------------------*/
%include "&_root_path./Sources/Modulos/m_psi/psi_report.sas";

%macro verify_psi(dataset1, dataset2);

    %local dsid1 dsid2 nobs1 nobs2 rc;
    %local has_byvar has_num has_cat can_execute;

    %let can_execute=0;

    /*=========================================
    1. Verificar que existan los datasets
    =========================================*/
    %if not %sysfunc(exist(&dataset1.)) %then %do;
        %put WARNING DEVELOPER: [PSI] Dataset TRAIN no existe: &dataset1.;
        %return;
    %end;

    %if not %sysfunc(exist(&dataset2.)) %then %do;
        %put WARNING DEVELOPER: [PSI] Dataset OOT no existe: &dataset2.;
        %return;
    %end;

    /*=========================================
    2. Verificar que tengan observaciones
    =========================================*/
    %let dsid1=%sysfunc(open(&dataset1.));
    %if &dsid1. > 0 %then %do;
        %let nobs1=%sysfunc(attrn(&dsid1., nobs));
        %let rc=%sysfunc(close(&dsid1.));
    %end;
    %else %let nobs1=0;

    %let dsid2=%sysfunc(open(&dataset2.));
    %if &dsid2. > 0 %then %do;
        %let nobs2=%sysfunc(attrn(&dsid2., nobs));
        %let rc=%sysfunc(close(&dsid2.));
    %end;
    %else %let nobs2=0;

    %if &nobs1.=0 %then %do;
        %put WARNING DEVELOPER: [PSI] Dataset TRAIN sin observaciones:
            &dataset1.;
        %return;
    %end;

    %if &nobs2.=0 %then %do;
        %put WARNING DEVELOPER: [PSI] Dataset OOT sin observaciones: &dataset2.;
        %return;
    %end;

    %put NOTE: [PSI] TRAIN: &nobs1. obs | OOT: &nobs2. obs;

    /*=========================================
    3. Verificar variable de tiempo (byvar)
    =========================================*/
    %let has_byvar=0;

    %if %length(&_var_time.) > 0 and &_var_time. ne . %then %do;
        /* Verificar que existe en ambos datasets */
        %let dsid1=%sysfunc(open(&dataset1.));
        %if &dsid1. > 0 %then %do;
            %if %sysfunc(varnum(&dsid1., &_var_time.)) > 0 %then %let has_byvar=
                1;
            %let rc=%sysfunc(close(&dsid1.));
        %end;

        %if &has_byvar.=1 %then %do;
            %let dsid2=%sysfunc(open(&dataset2.));
            %if &dsid2. > 0 %then %do;
                %if %sysfunc(varnum(&dsid2., &_var_time.))=0 %then %let
                    has_byvar=0;
                %let rc=%sysfunc(close(&dsid2.));
            %end;
        %end;
    %end;

    %if &has_byvar.=0 %then %do;
        %put WARNING DEVELOPER: [PSI] Variable de tiempo no definida o no existe
            en datasets: &_var_time.;
        %return;
    %end;

    %put NOTE: [PSI] Variable tiempo: &_var_time.;

    /*=========================================
    4. Verificar variables numéricas
    =========================================*/
    %let has_num=0;

    %if %length(&vars_num.) > 0 and &vars_num. ne . %then %do;
        %let has_num=1;
        %put NOTE: [PSI] Variables numéricas: &vars_num.;
    %end;

    /*=========================================
    5. Verificar variables categóricas
    =========================================*/
    %let has_cat=0;

    %if %length(&vars_cat.) > 0 and &vars_cat. ne . %then %do;
        %let has_cat=1;
        %put NOTE: [PSI] Variables categóricas: &vars_cat.;
    %end;

    /*=========================================
    6. Validar que exista al menos un tipo
    =========================================*/
    %if &has_num.=0 and &has_cat.=0 %then %do;
        %put WARNING DEVELOPER: [PSI] No hay variables numéricas ni categóricas
            definidas.;
        %return;
    %end;

    %let can_execute=1;

    %if &has_num.=1 and &has_cat.=1 %then %do;
        /* Caso 1: Ambos tipos de variables */
        %put NOTE: [PSI] Caso 1: Variables numéricas + categóricas;
        %__psi_report( t1=&dataset1., t2=&dataset2., byvar=&_var_time.,
            lista_var=&vars_num., lista_var_cat=&vars_cat. );
    %end;
    %else %if &has_num.=1 %then %do;
        /* Caso 2: Solo numéricas */
        %put NOTE: [PSI] Caso 2: Solo variables numéricas;
        %__psi_report( t1=&dataset1., t2=&dataset2., byvar=&_var_time.,
            lista_var=&vars_num., lista_var_cat=);
    %end;
    %else %if &has_cat.=1 %then %do;
        /* Caso 3: Solo categóricas */
        %put NOTE: [PSI] Caso 3: Solo variables categóricas;
        %__psi_report( t1=&dataset1., t2=&dataset2., byvar=&_var_time.,
            lista_var=, lista_var_cat=&vars_cat. );
    %end;

%mend verify_psi;
