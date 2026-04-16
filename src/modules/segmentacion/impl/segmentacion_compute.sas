/* =========================================================================
segmentacion_compute.sas - Computo del modulo Segmentacion (Metodo 3)

Flujo optimizado para CAS:
- la tabla canonica de analisis vive en CAS (`casuser._seg_input`)
- agregaciones, filtros y joins se hacen con PROC FEDSQL
- el ordenamiento se hace con table.partition
- `work` solo se usa para salidas de procedimientos no CAS-native
  (principalmente PROC NPAR1WAY)

Outputs principales (tablas CAS):
casuser._seg_mtd_global   - Materialidad consolidada y por periodo
casuser._seg_mtd_segm     - Materialidad por segmento y periodo
casuser._seg_mtd_resumen  - Resumen pct cumplimiento por periodo
casuser._seg_ks_results   - Resultados KS por par de segmentos
casuser._seg_ks_resumen   - Resumen KS
casuser._seg_kw_means     - Medias por segmento y periodo temporal
casuser._seg_kw_test      - Test Kruskal-Wallis
casuser._seg_mig_tipos    - Distribucion por tipo (CRUCE/RETIRADO/NUEVO)
casuser._seg_mig_cruce    - Matriz de migracion cruzada
casuser._seg_mig_resumen  - Resumen nuevos/retirados por segmento
========================================================================= */

%macro _seg_sort_cas(table_name=, orderby=, groupby={});

    %if %length(%superq(table_name)) = 0 or %length(%superq(orderby)) = 0 %then
        %return;

    proc cas;
        session conn;
        table.partition /
            table={
                caslib="casuser",
                name="&table_name.",
                orderby=&orderby.,
                groupby=&groupby.
            },
            casout={
                caslib="casuser",
                name="&table_name.",
                replace=true
            };
    quit;

%mend _seg_sort_cas;

%macro _seg_get_var_meta(data=, var=, out_type=, out_length=);

    %local _seg_lib _seg_mem;

    %let &out_type. = ;
    %let &out_length. = ;

    %let _seg_lib = %upcase(%scan(&data., 1, .));
    %let _seg_mem = %upcase(%scan(&data., 2, .));

    %if %length(%superq(_seg_mem)) = 0 %then %do;
        %let _seg_mem = &_seg_lib.;
        %let _seg_lib = WORK;
    %end;

    proc sql noprint;
        select max(upcase(type)),
               max(length)
          into :&out_type. trimmed,
               :&out_length. trimmed
        from dictionary.columns
        where upcase(libname) = "&_seg_lib."
          and upcase(memname) = "&_seg_mem."
          and upcase(name) = upcase("&var.");
    quit;

    %if %length(%superq(&out_type.)) = 0 %then
        %let &out_type. = NUM;
    %if %length(%superq(&out_length.)) = 0 %then
        %let &out_length. = 256;

%mend _seg_get_var_meta;

%macro _seg_materialidad(data=, target=, segvar=, period_var=_seg_period,
    min_obs=1000, min_target=450, has_segm=0);

    proc fedsql sessref=conn;
        create table casuser._seg_mtd_global {options replace=true} as
        select cast('CONSOLIDADO' as varchar(20)) as Periodo,
               count(*) as Materialidad,
               coalesce(sum(&target.), 0) as Cantidad_Target,
               case when count(*) >= &min_obs.
                    then 'CUMPLE' else 'NO CUMPLE'
               end as Verif_Materialidad,
               case when coalesce(sum(&target.), 0) >= &min_target.
                    then 'CUMPLE' else 'NO CUMPLE'
               end as Verif_Target
        from &data.
        union all
        select cast(&period_var. as varchar(20)) as Periodo,
               count(*) as Materialidad,
               coalesce(sum(&target.), 0) as Cantidad_Target,
               case when count(*) >= &min_obs.
                    then 'CUMPLE' else 'NO CUMPLE'
               end as Verif_Materialidad,
               case when coalesce(sum(&target.), 0) >= &min_target.
                    then 'CUMPLE' else 'NO CUMPLE'
               end as Verif_Target
        from &data.
        group by &period_var.;
    quit;

    %_seg_sort_cas(table_name=_seg_mtd_global,
        orderby=%str({"Periodo"}));

    %if &has_segm. = 1 %then %do;
        proc fedsql sessref=conn;
            create table casuser._seg_mtd_segm {options replace=true} as
            select cast('CONSOLIDADO' as varchar(20)) as Periodo,
                   &segvar. as Segmento,
                   count(*) as Materialidad,
                   coalesce(sum(&target.), 0) as Cantidad_Target,
                   case when count(*) >= &min_obs.
                        then 'CUMPLE' else 'NO CUMPLE'
                   end as Verif_Materialidad,
                   case when coalesce(sum(&target.), 0) >= &min_target.
                        then 'CUMPLE' else 'NO CUMPLE'
                   end as Verif_Target
            from &data.
            group by &segvar.
            union all
            select cast(&period_var. as varchar(20)) as Periodo,
                   &segvar. as Segmento,
                   count(*) as Materialidad,
                   coalesce(sum(&target.), 0) as Cantidad_Target,
                   case when count(*) >= &min_obs.
                        then 'CUMPLE' else 'NO CUMPLE'
                   end as Verif_Materialidad,
                   case when coalesce(sum(&target.), 0) >= &min_target.
                        then 'CUMPLE' else 'NO CUMPLE'
                   end as Verif_Target
            from &data.
            group by &period_var., &segvar.;
        quit;

        %_seg_sort_cas(table_name=_seg_mtd_segm,
            orderby=%str({"Periodo", "Segmento"}));

        proc fedsql sessref=conn;
            create table casuser._seg_mtd_resumen {options replace=true} as
            select Periodo,
                   count(*) as Total_Segmentos,
                   sum(case when Verif_Materialidad = 'CUMPLE' then 1 else 0 end)
                       as Cumplen_Materialidad,
                   sum(case when Verif_Target = 'CUMPLE' then 1 else 0 end)
                       as Cumplen_Target,
                   sum(case
                           when Verif_Materialidad = 'CUMPLE'
                            and Verif_Target = 'CUMPLE'
                           then 1 else 0
                       end) as Cumplen_Ambos,
                   case when count(*) > 0
                        then sum(case
                                    when Verif_Materialidad = 'CUMPLE'
                                     and Verif_Target = 'CUMPLE'
                                    then 1 else 0
                                 end) / count(*)
                        else 0
                   end as PCT_Cumplimiento
            from casuser._seg_mtd_segm
            group by Periodo;
        quit;

        %_seg_sort_cas(table_name=_seg_mtd_resumen,
            orderby=%str({"Periodo"}));
    %end;

%mend _seg_materialidad;

%macro _seg_kolmogorov(data=, segvar=, target=);

    %local n_segs i j seg_i seg_j n_obs dsid rc _seg_type _seg_len
        _seg_i_lit _seg_j_lit;

    %_seg_get_var_meta(data=&data., var=&segvar., out_type=_seg_type,
        out_length=_seg_len);

    proc sql noprint;
        select distinct &segvar.
          into :seg_val_1-:_seg_val_9999
        from &data.
        where not missing(&segvar.)
        order by &segvar.;
        %let n_segs = &sqlobs.;
    quit;

    data work._seg_ks_results;
        length Tipo_Muestra $20
            %if &_seg_type. = CHAR %then %do;
                Segmento1 $&_seg_len. Segmento2 $&_seg_len.
            %end;
            %else %do;
                Segmento1 8 Segmento2 8
            %end;
            KS_Statistic 8 KS_Asymptotic 8 D_Statistic 8
            P_Value 8 Prueba_KS $20;
        stop;
    run;

    %do i = 1 %to %eval(&n_segs. - 1);
        %let seg_i = &&_seg_val_&i.;
        %if &_seg_type. = CHAR %then
            %let _seg_i_lit = %sysfunc(quote(%superq(seg_i)));
        %else
            %let _seg_i_lit = %superq(seg_i);

        %do j = %eval(&i. + 1) %to &n_segs.;
            %let seg_j = &&_seg_val_&j.;
            %if &_seg_type. = CHAR %then
                %let _seg_j_lit = %sysfunc(quote(%superq(seg_j)));
            %else
                %let _seg_j_lit = %superq(seg_j);

            proc sql noprint;
                select count(*) into :n_obs trimmed
                from &data.
                where &segvar. in (&_seg_i_lit., &_seg_j_lit.);
            quit;

            %if &n_obs. > 10 %then %do;
                ods select none;
                proc npar1way data=&data.(where=(&segvar. in (&_seg_i_lit., &_seg_j_lit.))) KS;
                    class &segvar.;
                    var &target.;
                    output out=work._seg_ks_temp;
                run;
                ods select all;

                %let dsid = %sysfunc(open(work._seg_ks_temp));
                %if &dsid. > 0 %then %do;
                    %let rc = %sysfunc(close(&dsid.));

                    data work._seg_ks_row;
                        length Tipo_Muestra $20 Prueba_KS $20
                            %if &_seg_type. = CHAR %then %do;
                                Segmento1 $&_seg_len. Segmento2 $&_seg_len.
                            %end;
                            %else %do;
                                Segmento1 8 Segmento2 8
                            %end;
                            ;
                        set work._seg_ks_temp;
                        if _N_ = 1;
                        Tipo_Muestra = 'CONSOLIDADO';
                        %if &_seg_type. = CHAR %then %do;
                            Segmento1 = &_seg_i_lit.;
                            Segmento2 = &_seg_j_lit.;
                        %end;
                        %else %do;
                            Segmento1 = &seg_i.;
                            Segmento2 = &seg_j.;
                        %end;
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
                %put WARNING: [seg_kolmogorov] Insuficientes obs para par &seg_i. vs &seg_j. (n=&n_obs.).;
            %end;
        %end;
    %end;

    data casuser._seg_ks_results;
        set work._seg_ks_results;
    run;

    proc fedsql sessref=conn;
        create table casuser._seg_ks_resumen {options replace=true} as
        select cast('CONSOLIDADO' as varchar(20)) as Tipo_Muestra,
               count(*) as Total_Pares,
               sum(case when Prueba_KS = 'DIFERENTES' then 1 else 0 end)
                   as Pares_Diferentes,
               case when count(*) > 0
                    then sum(case when Prueba_KS = 'DIFERENTES' then 1 else 0 end) / count(*)
                    else 0
               end as Proporcion_Diferentes
        from casuser._seg_ks_results;
    quit;

    %_seg_sort_cas(table_name=_seg_ks_results,
        orderby=%str({"Segmento1", "Segmento2"}));

    proc datasets lib=work nolist nowarn;
        delete _seg_ks_temp _seg_ks_row;
    quit;

    %do i = 1 %to &n_segs.;
        %symdel _seg_val_&i. / nowarn;
    %end;

%mend _seg_kolmogorov;

%macro _seg_kruskall(data=, segvar=, target=, byvar=, period_var=_seg_period);

    %local total_obs_kw _seg_kw_nseg;

    proc sql noprint;
        select count(*) into :total_obs_kw trimmed from &data.;
    quit;

    %if %sysevalf(%superq(total_obs_kw)=, boolean) %then %let total_obs_kw = 0;

    proc fedsql sessref=conn;
        create table casuser._seg_kw_means {options replace=true} as
        select cast(&period_var. as varchar(20)) as Periodo,
               &segvar. as Segmento,
               &byvar. as Periodo_Temporal,
               count(*) as NObs,
               avg(&target.) as Media,
               case when &total_obs_kw. > 0
                    then round((count(*) / &total_obs_kw.) * 100, 0.01)
                    else 0
               end as Porcentaje
        from &data.
        group by &period_var., &segvar., &byvar.;
    quit;

    %_seg_sort_cas(table_name=_seg_kw_means,
        orderby=%str({"Periodo", "Segmento", "Periodo_Temporal"}));

    proc sql noprint;
        select count(distinct Segmento) into :_seg_kw_nseg trimmed
        from casuser._seg_kw_means;
    quit;

    %if %sysevalf(%superq(_seg_kw_nseg)=, boolean) %then %let _seg_kw_nseg = 0;

    %if &_seg_kw_nseg. > 1 %then %do;
        ods select none;
        proc npar1way data=casuser._seg_kw_means wilcoxon;
            class Segmento;
            var Media;
            ods output KruskalWallisTest=work._seg_kw_test;
        run;
        ods select all;

        data casuser._seg_kw_test;
            length Tipo_Muestra $20;
            set work._seg_kw_test;
            Tipo_Muestra = 'CONSOLIDADO';
        run;
    %end;
    %else %put WARNING: [seg_kruskall] Menos de 2 segmentos con datos. Se omite el test de Kruskal-Wallis.;

%mend _seg_kruskall;

%macro _seg_migracion(data=, idvar=, segvar=, byvar=, train_first_mes=,
    oot_last_mes=);

    %local _seg_ret_total _seg_new_total _seg_cross_total _seg_type_total;

    proc fedsql sessref=conn;
        create table casuser._seg_mig_pri {options replace=true} as
        select &idvar. as Id_Registro,
               &segvar. as Segmento_Train
        from &data.
        where &byvar. = &train_first_mes.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._seg_mig_ult {options replace=true} as
        select &idvar. as Id_Registro,
               &segvar. as Segmento_OOT
        from &data.
        where &byvar. = &oot_last_mes.;
    quit;

    proc fedsql sessref=conn;
        create table casuser._seg_mig_base {options replace=true} as
        select coalesce(a.Id_Registro, b.Id_Registro) as Id_Registro,
               a.Segmento_Train,
               b.Segmento_OOT,
               case
                   when a.Id_Registro is not null and b.Id_Registro is not null then 'CRUCE'
                   when a.Id_Registro is not null then 'RETIRADO'
                   else 'NUEVO'
               end as Tipo_Cliente
        from casuser._seg_mig_pri a
        full join casuser._seg_mig_ult b
            on a.Id_Registro = b.Id_Registro;
    quit;

    proc sql noprint;
        select count(*) into :_seg_ret_total trimmed
        from casuser._seg_mig_base where Tipo_Cliente = 'RETIRADO';
        select count(*) into :_seg_new_total trimmed
        from casuser._seg_mig_base where Tipo_Cliente = 'NUEVO';
        select count(*) into :_seg_cross_total trimmed
        from casuser._seg_mig_base where Tipo_Cliente = 'CRUCE';
        select count(*) into :_seg_type_total trimmed
        from casuser._seg_mig_base;
    quit;

    %if %sysevalf(%superq(_seg_ret_total)=, boolean) %then %let _seg_ret_total = 0;
    %if %sysevalf(%superq(_seg_new_total)=, boolean) %then %let _seg_new_total = 0;
    %if %sysevalf(%superq(_seg_cross_total)=, boolean) %then %let _seg_cross_total = 0;
    %if %sysevalf(%superq(_seg_type_total)=, boolean) %then %let _seg_type_total = 0;

    proc fedsql sessref=conn;
        create table casuser._seg_mig_ret {options replace=true} as
        select Segmento_Train as Segmento,
               count(*) as Cant_Retirados,
               case when &_seg_ret_total. > 0
                    then round((count(*) / &_seg_ret_total.) * 100, 0.01)
                    else 0
               end as Pct_Retirados
        from casuser._seg_mig_base
        where Tipo_Cliente = 'RETIRADO'
        group by Segmento_Train;
    quit;

    proc fedsql sessref=conn;
        create table casuser._seg_mig_nue {options replace=true} as
        select Segmento_OOT as Segmento,
               count(*) as Cant_Nuevos,
               case when &_seg_new_total. > 0
                    then round((count(*) / &_seg_new_total.) * 100, 0.01)
                    else 0
               end as Pct_Nuevos
        from casuser._seg_mig_base
        where Tipo_Cliente = 'NUEVO'
        group by Segmento_OOT;
    quit;

    proc fedsql sessref=conn;
        create table casuser._seg_mig_resumen {options replace=true} as
        select coalesce(a.Segmento, b.Segmento) as Segmento,
               coalesce(a.Cant_Retirados, 0) as Cant_Retirados,
               coalesce(a.Pct_Retirados, 0) as Pct_Retirados,
               coalesce(b.Cant_Nuevos, 0) as Cant_Nuevos,
               coalesce(b.Pct_Nuevos, 0) as Pct_Nuevos
        from casuser._seg_mig_ret a
        full join casuser._seg_mig_nue b
            on a.Segmento = b.Segmento;
    quit;

    proc fedsql sessref=conn;
        create table casuser._seg_mig_cruce {options replace=true} as
        select Segmento_Train as Segmento_Inicial,
               Segmento_OOT as Segmento_Final,
               count(*) as Frequency,
               case when &_seg_cross_total. > 0
                    then round((count(*) / &_seg_cross_total.) * 100, 0.01)
                    else 0
               end as Percent
        from casuser._seg_mig_base
        where Tipo_Cliente = 'CRUCE'
        group by Segmento_Train, Segmento_OOT;
    quit;

    proc fedsql sessref=conn;
        create table casuser._seg_mig_tipos {options replace=true} as
        select cast('CONSOLIDADO' as varchar(20)) as Tipo_Muestra,
               Tipo_Cliente as Indicador,
               count(*) as Frequency,
               case when &_seg_type_total. > 0
                    then round((count(*) / &_seg_type_total.) * 100, 0.01)
                    else 0
               end as PCT_Total
        from casuser._seg_mig_base
        group by Tipo_Cliente;
    quit;

    %_seg_sort_cas(table_name=_seg_mig_resumen,
        orderby=%str({"Segmento"}));
    %_seg_sort_cas(table_name=_seg_mig_cruce,
        orderby=%str({"Segmento_Inicial", "Segmento_Final"}));
    %_seg_sort_cas(table_name=_seg_mig_tipos,
        orderby=%str({"Indicador"}));

    proc datasets lib=casuser nolist nowarn;
        delete _seg_mig_pri _seg_mig_ult _seg_mig_base
            _seg_mig_ret _seg_mig_nue;
    quit;

%mend _seg_migracion;

%macro _seg_compute(data=, target=, segvar=, byvar=, idvar=,
    min_obs=1000, min_target=450, train_first_mes=, oot_last_mes=,
    has_segm=0, has_id=0, period_var=_seg_period);

    %put NOTE: [seg_compute] Inicio - has_segm=&has_segm. has_id=&has_id.;

    %_seg_materialidad(data=&data., target=&target., segvar=&segvar.,
        period_var=&period_var., min_obs=&min_obs.,
        min_target=&min_target., has_segm=&has_segm.);

    %if &has_segm. = 1 %then %do;
        %_seg_kruskall(data=&data., segvar=&segvar., target=&target.,
            byvar=&byvar., period_var=&period_var.);
        %_seg_kolmogorov(data=&data., segvar=&segvar., target=&target.);
    %end;

    %if &has_segm. = 1 and &has_id. = 1
        and %length(%superq(train_first_mes)) > 0
        and %length(%superq(oot_last_mes)) > 0 %then %do;
        %_seg_migracion(data=&data., idvar=&idvar., segvar=&segvar.,
            byvar=&byvar., train_first_mes=&train_first_mes.,
            oot_last_mes=&oot_last_mes.);
    %end;

    %put NOTE: [seg_compute] Fin.;

%mend _seg_compute;
