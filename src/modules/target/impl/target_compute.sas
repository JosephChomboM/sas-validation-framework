/* =========================================================================
target_compute.sas - Computo CAS-first para analisis del target
========================================================================= */

%macro _target_partition(name=, orderby=, groupby=);
    %local _group_clause;
    %if %length(%superq(groupby)) > 0 %then
        %let _group_clause=groupby={&groupby.};
    %else %let _group_clause=groupby={};

    proc cas;
        session conn;
        table.partition /
            table={caslib="casuser", name="&name.", orderby={&orderby.},
                &_group_clause.},
            casout={caslib="casuser", name="&name.", replace=true};
    quit;
%mend _target_partition;

%macro _target_prepare_base(train_data=, oot_data=, byvar=, def_cld=0);
    proc fedsql sessref=conn;
        create table casuser._tgt_train_filt {options replace=true} as
        select 'TRAIN' as Muestra, *
        from &train_data.
        %if &def_cld. ne 0 %then %do;
            where &byvar. <= &def_cld.
        %end;
        ;

        create table casuser._tgt_oot_filt {options replace=true} as
        select 'OOT' as Muestra, *
        from &oot_data.
        %if &def_cld. ne 0 %then %do;
            where &byvar. <= &def_cld.
        %end;
        ;

        create table casuser._tgt_base {options replace=true} as
        select * from casuser._tgt_train_filt
        union all
        select * from casuser._tgt_oot_filt;
    quit;

    %_target_partition(name=_tgt_base,
        orderby=%str("Muestra","&byvar."));
%mend _target_prepare_base;

%macro _target_build_describe(data=casuser._tgt_base, target=, byvar=);
    proc fedsql sessref=conn;
        create table casuser._tgt_describe {options replace=true} as
        select Muestra,
            &byvar.,
            count(*) as N,
            avg(cast(&target. as double)) as avg_target
        from &data.
        group by Muestra, &byvar.;

        create table casuser._tgt_materialidad {options replace=true} as
        select Muestra,
            &byvar.,
            cast(&target. as double) as Valor_Target,
            count(*) as N
        from &data.
        group by Muestra, &byvar., &target.;
    quit;

    %_target_partition(name=_tgt_describe,
        orderby=%str("Muestra","&byvar."));
    %_target_partition(name=_tgt_materialidad,
        orderby=%str("Muestra","&byvar.","Valor_Target"));

    %local _muestra _nobs _ncompare _first_mean _last_mean _relative_diff;
    data casuser._tgt_diff_rel;
        length Muestra $5 Metric $40 Value 8;
        stop;
    run;

    %do _j=1 %to 2;
        %if &_j.=1 %then %let _muestra=TRAIN;
        %else %let _muestra=OOT;

        proc sql noprint;
            select count(*) into :_nobs trimmed
            from casuser._tgt_describe
            where Muestra="&_muestra.";
        quit;

        %if %sysevalf(&_nobs. > 1) %then %do;
            %if &_nobs. >= 6 %then %let _ncompare=3;
            %else %let _ncompare=1;

            proc sql noprint outobs=&_ncompare.;
                select avg_target into :_tgt_first1- trimmed
                from casuser._tgt_describe
                where Muestra="&_muestra."
                order by &byvar.;
            quit;

            proc sql noprint outobs=&_ncompare.;
                select avg_target into :_tgt_last1- trimmed
                from casuser._tgt_describe
                where Muestra="&_muestra."
                order by &byvar. desc;
            quit;

            %let _first_mean=0;
            %let _last_mean=0;
            %do _k=1 %to &_ncompare.;
                %let _first_mean=%sysevalf(&_first_mean. + &&_tgt_first&_k.);
                %let _last_mean=%sysevalf(&_last_mean. + &&_tgt_last&_k.);
            %end;
            %let _first_mean=%sysevalf(&_first_mean. / &_ncompare.);
            %let _last_mean=%sysevalf(&_last_mean. / &_ncompare.);

            %if &_first_mean. ne 0 %then
                %let _relative_diff=%sysevalf((&_last_mean. - &_first_mean.) / &_first_mean.);
            %else %let _relative_diff=0;

            data casuser._tgt_diff_tmp;
                length Muestra $5 Metric $40 Value 8;
                Muestra="&_muestra.";
                %if &_ncompare. = 3 %then %do;
                    Metric="Promedio primeros 3 meses";
                %end;
                %else %do;
                    Metric="Primer mes";
                %end;
                Value=&_first_mean.; output;
                %if &_ncompare. = 3 %then %do;
                    Metric="Promedio ultimos 3 meses";
                %end;
                %else %do;
                    Metric="Ultimo mes";
                %end;
                Value=&_last_mean.; output;
                Metric="Diferencia relativa"; Value=&_relative_diff.; output;
            run;

            proc cas;
                session conn;
                table.append /
                    source={caslib="casuser", name="_tgt_diff_tmp"},
                    target={caslib="casuser", name="_tgt_diff_rel"};
            quit;
        %end;
    %end;

    %_target_partition(name=_tgt_diff_rel,
        orderby=%str("Muestra","Metric"));
%mend _target_build_describe;

%macro _target_build_bandas(data=casuser._tgt_base, target=, byvar=);
    proc fedsql sessref=conn;
        create table casuser._tgt_bandas_stats {options replace=true} as
        select a.global_avg,
            coalesce(b.std_monthly, 0) as std_monthly,
            a.global_avg - 2 * coalesce(b.std_monthly, 0) as lower_band,
            a.global_avg + 2 * coalesce(b.std_monthly, 0) as upper_band
        from (
            select avg(cast(&target. as double)) as global_avg
            from &data.
            where Muestra='TRAIN'
        ) a
        cross join (
            select std(avg_target) as std_monthly
            from casuser._tgt_describe
            where Muestra='TRAIN'
        ) b;

        create table casuser._tgt_bandas {options replace=true} as
        select d.Muestra,
            d.&byvar.,
            d.avg_target,
            s.lower_band,
            s.upper_band,
            s.global_avg
        from casuser._tgt_describe d
        cross join casuser._tgt_bandas_stats s;

        create table casuser._tgt_bandas_ref {options replace=true} as
        select distinct d.&byvar.,
            s.lower_band,
            s.upper_band,
            s.global_avg
        from casuser._tgt_describe d
        cross join casuser._tgt_bandas_stats s;
    quit;

    %_target_partition(name=_tgt_bandas,
        orderby=%str("Muestra","&byvar."));
    %_target_partition(name=_tgt_bandas_ref,
        orderby=%str("&byvar."));
%mend _target_build_bandas;

%macro _target_build_ponderado_promedio(data=casuser._tgt_base, target=, monto=, byvar=);
    %if %length(%superq(monto))=0 %then %return;

    proc fedsql sessref=conn;
        create table casuser._tgt_pond_prom {options replace=true} as
        select Muestra,
            &byvar.,
            sum(cast(&target. as double) * cast(&monto. as double)) /
                sum(cast(&monto. as double)) as avg_target_pond
        from &data.
        where &monto. > 0
        group by Muestra, &byvar.;

        create table casuser._tgt_pond_prom_stats {options replace=true} as
        select a.global_mean,
            coalesce(b.std_monthly, 0) as std_monthly,
            a.global_mean - 2 * coalesce(b.std_monthly, 0) as lower_band,
            a.global_mean + 2 * coalesce(b.std_monthly, 0) as upper_band
        from (
            select sum(cast(&target. as double) * cast(&monto. as double)) /
                sum(cast(&monto. as double)) as global_mean
            from &data.
            where Muestra='TRAIN' and &monto. > 0
        ) a
        cross join (
            select std(avg_target_pond) as std_monthly
            from casuser._tgt_pond_prom
            where Muestra='TRAIN'
        ) b;

        create table casuser._tgt_pond_prom_bandas {options replace=true} as
        select d.Muestra,
            d.&byvar.,
            d.avg_target_pond,
            s.lower_band,
            s.upper_band,
            s.global_mean
        from casuser._tgt_pond_prom d
        cross join casuser._tgt_pond_prom_stats s;
    quit;

    %_target_partition(name=_tgt_pond_prom_bandas,
        orderby=%str("Muestra","&byvar."));
%mend _target_build_ponderado_promedio;

%macro _target_build_ponderado_suma(data=casuser._tgt_base, target=, monto=, byvar=);
    %if %length(%superq(monto))=0 %then %return;

    proc fedsql sessref=conn;
        create table casuser._tgt_sum_pond {options replace=true} as
        select Muestra,
            &byvar.,
            sum(cast(&target. as double) * cast(&monto. as double)) as sum_target_pond,
            sum(cast(&monto. as double)) as total_monto
        from &data.
        group by Muestra, &byvar.;

        create table casuser._tgt_sum_pond_stats {options replace=true} as
        select a.global_mean,
            coalesce(b.std_monthly, 0) as std_monthly,
            a.global_mean - 2 * coalesce(b.std_monthly, 0) as lower_band,
            a.global_mean + 2 * coalesce(b.std_monthly, 0) as upper_band
        from (
            select avg(sum_target_pond) as global_mean
            from casuser._tgt_sum_pond
            where Muestra='TRAIN'
        ) a
        cross join (
            select std(sum_target_pond) as std_monthly
            from casuser._tgt_sum_pond
            where Muestra='TRAIN'
        ) b;

        create table casuser._tgt_sum_pond_bandas {options replace=true} as
        select d.Muestra,
            d.&byvar.,
            d.sum_target_pond,
            d.total_monto,
            s.lower_band,
            s.upper_band,
            s.global_mean
        from casuser._tgt_sum_pond d
        cross join casuser._tgt_sum_pond_stats s;

        create table casuser._tgt_ratio {options replace=true} as
        select Muestra,
            &byvar.,
            case when total_monto > 0 then sum_target_pond / total_monto else null
            end as ratio_default_monto
        from casuser._tgt_sum_pond;

        create table casuser._tgt_ratio_stats {options replace=true} as
        select a.global_mean,
            coalesce(b.std_monthly, 0) as std_monthly,
            a.global_mean - 2 * coalesce(b.std_monthly, 0) as lower_band,
            a.global_mean + 2 * coalesce(b.std_monthly, 0) as upper_band
        from (
            select avg(ratio_default_monto) as global_mean
            from casuser._tgt_ratio
            where Muestra='TRAIN' and ratio_default_monto is not null
        ) a
        cross join (
            select std(ratio_default_monto) as std_monthly
            from casuser._tgt_ratio
            where Muestra='TRAIN' and ratio_default_monto is not null
        ) b;

        create table casuser._tgt_ratio_bandas {options replace=true} as
        select d.Muestra,
            d.&byvar.,
            d.ratio_default_monto,
            s.lower_band,
            s.upper_band,
            s.global_mean
        from casuser._tgt_ratio d
        cross join casuser._tgt_ratio_stats s
        where d.ratio_default_monto is not null;
    quit;

    %_target_partition(name=_tgt_sum_pond_bandas,
        orderby=%str("Muestra","&byvar."));
    %_target_partition(name=_tgt_ratio_bandas,
        orderby=%str("Muestra","&byvar."));
%mend _target_build_ponderado_suma;
