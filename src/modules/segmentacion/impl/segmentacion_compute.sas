/* =========================================================================
segmentacion_compute.sas - Computo del modulo Segmentacion (Metodo 3)

Macros:
%_seg_materialidad  - Verificacion de suficiencia (obs + target por segmento)
%_seg_kolmogorov    - Test KS de heterogeneidad entre pares de segmentos
%_seg_kruskall      - Test de Kruskal-Wallis para diferencias entre segmentos
%_seg_migracion     - Analisis de migracion de segmentos entre periodos
%_seg_compute       - Orquestador

Todas las operaciones usan Pattern B (work staging):
PROC NPAR1WAY, PROC SORT, PROC FREQ, PROC MEANS, PROC APPEND
no son CAS-compatibles.

Outputs (tablas work):
work._seg_mtd_global   - Materialidad global
work._seg_mtd_segm     - Materialidad por segmento (si existen)
work._seg_mtd_resumen  - Resumen pct cumplimiento
work._seg_ks_results   - Resultados KS por par de segmentos
work._seg_ks_resumen   - Resumen KS
work._seg_kw_means     - Medias por segmento y byvar
work._seg_kw_test      - Test Kruskal-Wallis
work._seg_mig_tipos    - Distribucion por tipo (CRUCE/RETIRADO/NUEVO)
work._seg_mig_cruce    - Matriz de migracion cruzada
work._seg_mig_resumen  - Resumen nuevos/retirados por segmento
========================================================================= */

/* =====================================================================
%_seg_materialidad - Verificacion de suficiencia

Entrada: data work, target, segvar, data_type, min_obs, min_target, has_segm
Salida:  _seg_mtd_global (siempre)
         _seg_mtd_segm, _seg_mtd_resumen (solo si has_segm=1)
===================================================================== */
%macro _seg_materialidad(data=, target=, segvar=, data_type=, min_obs=1000,
    min_target=450, has_segm=0);

    %local total_obs total_target;

    proc sql noprint;
        select count(*) into :total_obs trimmed from &data.;
        select sum(&target.) into :total_target trimmed from &data.;
    quit;

    data work._seg_mtd_global;
        length Tipo_Muestra $20 Verif_Materialidad $10 Verif_Target $10;
        Tipo_Muestra = "&data_type.";
        Materialidad = &total_obs.;
        Cantidad_Target = &total_target.;
        if Materialidad >= &min_obs. then Verif_Materialidad = 'CUMPLE';
        else Verif_Materialidad = 'NO CUMPLE';
        if Cantidad_Target >= &min_target. then Verif_Target = 'CUMPLE';
        else Verif_Target = 'NO CUMPLE';
    run;

    %if &has_segm. = 1 %then %do;

        proc sql noprint;
            create table work._seg_mtd_segm as
            select
                &segvar. as Segmento,
                count(*) as Materialidad,
                sum(&target.) as Cantidad_Target,
                case when calculated Materialidad >= &min_obs.
                    then 'CUMPLE' else 'NO CUMPLE'
                end as Verif_Materialidad length=10,
                case when calculated Cantidad_Target >= &min_target.
                    then 'CUMPLE' else 'NO CUMPLE'
                end as Verif_Target length=10
            from &data.
            group by &segvar.
            order by &segvar.;
        quit;

        %local total_segs cumplen_mat cumplen_tgt cumplen_ambos;

        proc sql noprint;
            select count(distinct Segmento) into :total_segs trimmed
                from work._seg_mtd_segm;
            select count(*) into :cumplen_mat trimmed
                from work._seg_mtd_segm
                where Verif_Materialidad = 'CUMPLE';
            select count(*) into :cumplen_tgt trimmed
                from work._seg_mtd_segm
                where Verif_Target = 'CUMPLE';
            select count(*) into :cumplen_ambos trimmed
                from work._seg_mtd_segm
                where Verif_Materialidad = 'CUMPLE'
                  and Verif_Target = 'CUMPLE';
        quit;

        data work._seg_mtd_resumen;
            length Tipo_Muestra $20;
            Tipo_Muestra = "&data_type.";
            Total_Segmentos = &total_segs.;
            Cumplen_Materialidad = &cumplen_mat.;
            Cumplen_Target = &cumplen_tgt.;
            Cumplen_Ambos = &cumplen_ambos.;
            PCT_Cumplimiento = Cumplen_Ambos / Total_Segmentos;
            format PCT_Cumplimiento percent8.2;
        run;

    %end;

%mend _seg_materialidad;

/* =====================================================================
%_seg_kolmogorov - Test KS de heterogeneidad entre pares de segmentos

Usa PROC NPAR1WAY con OUTPUT OUT= para extraer _KS_, _KSA_, _D_, P_KSA.
Itera todas las combinaciones C(n,2) de segmentos.
P_KSA < 0.05 => DIFERENTES (segmentos heterogeneos).

Entrada: data work, segvar, target, data_type
Salida:  _seg_ks_results (detalle por par), _seg_ks_resumen (resumen)
===================================================================== */
%macro _seg_kolmogorov(data=, segvar=, target=, data_type=);

    %local seg_list n_segs i j seg_i seg_j n_obs dsid rc;

    proc sql noprint;
        select distinct &segvar. into :seg_list separated by ' '
        from &data.;
        select count(distinct &segvar.) into :n_segs trimmed
        from &data.;
    quit;

    /* Inicializar tabla de resultados con estructura */
    data work._seg_ks_results;
        length Tipo_Muestra $20 Segmento1 8 Segmento2 8
            KS_Statistic 8 KS_Asymptotic 8 D_Statistic 8
            P_Value 8 Prueba_KS $20;
        stop;
    run;

    %do i = 1 %to %eval(&n_segs. - 1);
        %let seg_i = %scan(&seg_list., &i.);

        %do j = %eval(&i. + 1) %to &n_segs.;
            %let seg_j = %scan(&seg_list., &j.);

            data work._seg_ks_pair;
                set &data.;
                where &segvar. in (&seg_i., &seg_j.);
                keep &segvar. &target.;
            run;

            proc sql noprint;
                select count(*) into :n_obs trimmed
                from work._seg_ks_pair;
            quit;

            %if &n_obs. > 10 %then %do;

                ods select none;
                proc npar1way data=work._seg_ks_pair KS;
                    class &segvar.;
                    var &target.;
                    output out=work._seg_ks_temp;
                run;
                ods select all;

                %let dsid = %sysfunc(open(work._seg_ks_temp));
                %if &dsid. > 0 %then %do;
                    %let rc = %sysfunc(close(&dsid.));

                    data work._seg_ks_row;
                        length Tipo_Muestra $20 Prueba_KS $20;
                        set work._seg_ks_temp;
                        if _N_ = 1;
                        Tipo_Muestra = "&data_type.";
                        Segmento1 = &seg_i.;
                        Segmento2 = &seg_j.;
                        KS_Statistic = _KS_;
                        KS_Asymptotic = _KSA_;
                        D_Statistic = _D_;
                        P_Value = P_KSA;
                        if P_Value < 0.05 then Prueba_KS = 'DIFERENTES';
                        else Prueba_KS = 'SIMILARES';
                        keep Tipo_Muestra Segmento1 Segmento2
                            KS_Statistic KS_Asymptotic D_Statistic
                            P_Value Prueba_KS;
                    run;

                    proc append base=work._seg_ks_results
                        data=work._seg_ks_row force;
                    run;
                %end;

            %end;
            %else %do;
                %put WARNING: [seg_kolmogorov] Insuficientes obs para
                    par &seg_i. vs &seg_j. (n=&n_obs.).;
            %end;

        %end;
    %end;

    /* Resumen de heterogeneidad */
    proc sql;
        create table work._seg_ks_resumen as
        select
            "&data_type." as Tipo_Muestra length=20,
            count(*) as Total_Pares,
            sum(case when Prueba_KS = 'DIFERENTES' then 1 else 0 end)
                as Pares_Diferentes,
            calculated Pares_Diferentes / calculated Total_Pares
                as Proporcion_Diferentes format=percent8.1
        from work._seg_ks_results;
    quit;

    /* Cleanup intermedias */
    proc datasets lib=work nolist nowarn;
        delete _seg_ks_pair _seg_ks_temp _seg_ks_row;
    quit;

%mend _seg_kolmogorov;

/* =====================================================================
%_seg_kruskall - Test de Kruskal-Wallis

Calcula medias de target por segmento*byvar, luego aplica PROC NPAR1WAY
sobre las medias agrupadas. Prueba si la distribucion de medias difiere
entre segmentos.

Entrada: data work, segvar, target, byvar
Salida:  _seg_kw_means (medias), _seg_kw_test (test)
===================================================================== */
%macro _seg_kruskall(data=, segvar=, target=, byvar=);

    %local total_obs_kw;

    proc sql noprint;
        select count(*) into :total_obs_kw trimmed from &data.;
    quit;

    proc means data=&data. noprint;
        class &segvar. &byvar.;
        var &target.;
        output out=work._seg_kw_means(where=(_TYPE_ = 3))
            n=NObs mean=Media;
    run;

    data work._seg_kw_means;
        set work._seg_kw_means;
        Porcentaje = round((NObs / &total_obs_kw.) * 100, 0.01);
        drop _TYPE_ _FREQ_;
    run;

    ods select none;
    proc npar1way data=work._seg_kw_means wilcoxon;
        class &segvar.;
        var Media;
        ods output KruskalWallisTest=work._seg_kw_test;
    run;
    ods select all;

%mend _seg_kruskall;

/* =====================================================================
%_seg_migracion - Analisis de migracion de segmentos

Compara asignacion de segmentos entre primer_mes y ultimo_mes.
Clasifica cuentas como CRUCE (permanece), RETIRADO (sale) o NUEVO (entra).
Genera matriz de migracion cruzada entre segmentos.

Entrada: data work, idvar, segvar, byvar, primer_mes, ultimo_mes, data_type
Salida:  _seg_mig_resumen (nuevos/retirados por segmento)
         _seg_mig_cruce (cross-tab para heatmap)
         _seg_mig_tipos (distribucion CRUCE/RETIRADO/NUEVO)
===================================================================== */
%macro _seg_migracion(data=, idvar=, segvar=, byvar=, primer_mes=,
    ultimo_mes=, data_type=);

    /* Subsets primer y ultimo mes */
    data work._seg_m_pri;
        set &data.;
        where &byvar. = &primer_mes.;
        rename &segvar. = seg_primer_mes;
        keep &idvar. &segvar.;
    run;

    proc sort data=work._seg_m_pri;
        by &idvar.;
    run;

    data work._seg_m_ult;
        set &data.;
        where &byvar. = &ultimo_mes.;
        rename &segvar. = seg_ultimo_mes;
        keep &idvar. &segvar.;
    run;

    proc sort data=work._seg_m_ult;
        by &idvar.;
    run;

    /* Merge por ID para clasificar migracion */
    data work._seg_m_merge;
        merge work._seg_m_pri(in=a) work._seg_m_ult(in=b);
        by &idvar.;
        length tipo_cliente $10;
        if a and b then tipo_cliente = 'CRUCE';
        else if a then tipo_cliente = 'RETIRADO';
        else if b then tipo_cliente = 'NUEVO';
    run;

    /* Retirados por segmento */
    ods select none;
    proc freq data=work._seg_m_merge;
        where tipo_cliente = 'RETIRADO';
        tables seg_primer_mes / nocum nocol;
        ods output OneWayFreqs=work._seg_m_ret;
    run;

    data work._seg_m_ret;
        set work._seg_m_ret;
        rename seg_primer_mes = Segmento
            Frequency = Cant_Retirados
            Percent = Pct_Retirados;
        keep seg_primer_mes Frequency Percent;
    run;

    /* Nuevos por segmento */
    proc freq data=work._seg_m_merge;
        where tipo_cliente = 'NUEVO';
        tables seg_ultimo_mes / nocum nocol;
        ods output OneWayFreqs=work._seg_m_nue;
    run;
    ods select all;

    data work._seg_m_nue;
        set work._seg_m_nue;
        rename seg_ultimo_mes = Segmento
            Frequency = Cant_Nuevos
            Percent = Pct_Nuevos;
        keep seg_ultimo_mes Frequency Percent;
    run;

    /* Resumen: retirados + nuevos por segmento */
    data work._seg_mig_resumen;
        merge work._seg_m_ret(in=a) work._seg_m_nue(in=b);
        by Segmento;
    run;

    /* Matriz de migracion cruzada (para heatmap) */
    proc freq data=work._seg_m_merge noprint;
        where tipo_cliente = 'CRUCE';
        tables seg_primer_mes * seg_ultimo_mes /
            nocol nocum nopercent out=work._seg_mig_cruce;
    run;

    /* Distribucion por tipo de cliente */
    ods select none;
    proc freq data=work._seg_m_merge;
        tables tipo_cliente / nocum;
        ods output OneWayFreqs=work._seg_m_props;
    run;
    ods select all;

    data work._seg_mig_tipos;
        length Indicador $40 Tipo_Muestra $20;
        set work._seg_m_props(rename=(
            tipo_cliente = Indicador
            Frequency = Frequency
            Percent = PCT_Total));
        Tipo_Muestra = "&data_type.";
        keep Tipo_Muestra Indicador Frequency PCT_Total;
    run;

    /* Cleanup intermedias */
    proc datasets lib=work nolist nowarn;
        delete _seg_m_pri _seg_m_ult _seg_m_merge
            _seg_m_ret _seg_m_nue _seg_m_props;
    quit;

%mend _seg_migracion;

/* =====================================================================
%_seg_compute - Orquestador principal

Ejecuta todos los sub-computos segun has_segm flag.
Materialidad siempre ejecuta; KS, Kruskal, migracion solo si has_segm=1.
===================================================================== */
%macro _seg_compute(data=, target=, segvar=, byvar=, idvar=, data_type=,
    min_obs=1000, min_target=450, primer_mes=, ultimo_mes=, has_segm=0);

    %put NOTE: [seg_compute] Inicio - data_type=&data_type.
        has_segm=&has_segm.;

    /* 1) Materialidad (siempre) */
    %_seg_materialidad(data=&data., target=&target., segvar=&segvar.,
        data_type=&data_type., min_obs=&min_obs., min_target=&min_target.,
        has_segm=&has_segm.);

    /* 2) Kruskal-Wallis (solo si hay segmentos) */
    %if &has_segm. = 1 %then %do;
        %_seg_kruskall(data=&data., segvar=&segvar., target=&target.,
            byvar=&byvar.);
    %end;

    /* 3) KS test entre pares de segmentos */
    %if &has_segm. = 1 %then %do;
        %_seg_kolmogorov(data=&data., segvar=&segvar., target=&target.,
            data_type=&data_type.);
    %end;

    /* 4) Migracion (solo si segmentos + periodos definidos) */
    %if &has_segm. = 1
        and %length(&primer_mes.) > 0
        and %length(&ultimo_mes.) > 0 %then %do;
        %_seg_migracion(data=&data., idvar=&idvar., segvar=&segvar.,
            byvar=&byvar., primer_mes=&primer_mes., ultimo_mes=&ultimo_mes.,
            data_type=&data_type.);
    %end;

    %put NOTE: [seg_compute] Fin - &data_type.;

%mend _seg_compute;
