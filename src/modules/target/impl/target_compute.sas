/* =========================================================================
target_compute.sas - Computo CAS-first para analisis de Target

Se prioriza minimizar pasos CAS. El ordenamiento se difiere al reporte.
========================================================================= */

%macro _target_build_base(train_data=, oot_data=, byvar=, def_cld=);
    proc fedsql sessref=conn;
        create table casuser._tgt_base {options replace=true} as
        select 'TRAIN' as Muestra, *
        from &train_data.
        where &byvar. <= &def_cld.
        union all
        select 'OOT' as Muestra, *
        from &oot_data.
        where &byvar. <= &def_cld.;
    quit;
%mend _target_build_base;

%macro _target_build_describe(data=casuser._tgt_base, target=, byvar=);
    proc fedsql sessref=conn;
        create table casuser._tgt_describe {options replace=true} as
        select Muestra,
            &byvar.,
            count(cast(&target. as double)) as N,
            avg(cast(&target. as double)) as avg_target
        from &data.
        group by Muestra, &byvar.;

        create table casuser._tgt_materialidad {options replace=true} as
        select Muestra,
            &byvar.,
            cast(&target. as double) as Valor_Target,
            count(*) as N
        from &data.
        where cast(&target. as double) is not null
        group by Muestra, &byvar., cast(&target. as double);
    quit;

%mend _target_build_describe;

%macro _target_build_diff_relative(byvar=);
    %local _sample _n_months _n_compare _first_mean _last_mean _rel_diff
        _i _tmpval;

    data casuser._tgt_diff_rel;
        length Muestra $5 Metric $40 Value 8;
        stop;
    run;

    %do _i=1 %to 2;
        %if &_i.=1 %then %let _sample=TRAIN;
        %else %let _sample=OOT;

        %let _n_months=0;
        proc sql noprint;
            select count(*) into :_n_months trimmed
            from casuser._tgt_describe
            where Muestra="&_sample.";
        quit;

        %if %sysevalf(&_n_months. > 1) %then %do;
            %if &_n_months. >= 6 %then %let _n_compare=3;
            %else %let _n_compare=1;

            %let _first_mean=0;
            proc sql noprint outobs=&_n_compare.;
                select avg_target into :_tgt_first1-:_tgt_first&_n_compare.
                from casuser._tgt_describe
                where Muestra="&_sample."
                order by &byvar.;
            quit;

            %do _j=1 %to &_n_compare.;
                %let _tmpval=&&_tgt_first&_j.;
                %if %length(%superq(_tmpval))=0 %then %let _tmpval=0;
                %let _first_mean=%sysevalf(&_first_mean. + &_tmpval.);
            %end;
            %let _first_mean=%sysevalf(&_first_mean. / &_n_compare.);

            %let _last_mean=0;
            proc sql noprint outobs=&_n_compare.;
                select avg_target into :_tgt_last1-:_tgt_last&_n_compare.
                from casuser._tgt_describe
                where Muestra="&_sample."
                order by &byvar. desc;
            quit;

            %do _j=1 %to &_n_compare.;
                %let _tmpval=&&_tgt_last&_j.;
                %if %length(%superq(_tmpval))=0 %then %let _tmpval=0;
                %let _last_mean=%sysevalf(&_last_mean. + &_tmpval.);
            %end;
            %let _last_mean=%sysevalf(&_last_mean. / &_n_compare.);

            %if %sysevalf(&_first_mean. ne 0) %then
                %let _rel_diff=%sysevalf((&_last_mean. - &_first_mean.) / &_first_mean.);
            %else %let _rel_diff=.;

            data casuser._tgt_diff_tmp;
                length Muestra $5 Metric $40 Value 8;
                Muestra="&_sample.";
                %if &_n_compare.=3 %then %do;
                    Metric="Promedio primeros 3 meses";
                %end;
                %else %do;
                    Metric="Primer mes";
                %end;
                Value=&_first_mean.; output;

                %if &_n_compare.=3 %then %do;
                    Metric="Promedio ultimos 3 meses";
                %end;
                %else %do;
                    Metric="Ultimo mes";
                %end;
                Value=&_last_mean.; output;

                Metric="Diferencia relativa";
                Value=&_rel_diff.;
                output;
            run;

            proc cas;
                session conn;
                table.append /
                    source={caslib="casuser", name="_tgt_diff_tmp"},
                    target={caslib="casuser", name="_tgt_diff_rel"};
            quit;
        %end;
    %end;
%mend _target_build_diff_relative;

%macro _target_build_rd_bands(data=casuser._tgt_base, target=, byvar=);
    %global _tgt_global_avg _tgt_rd_lower _tgt_rd_upper;
    %local _tgt_std_monthly;
    %let _tgt_global_avg=.;
    %let _tgt_rd_lower=.;
    %let _tgt_rd_upper=.;
    %let _tgt_std_monthly=.;

    proc sql noprint;
        select global_avg into :_tgt_global_avg trimmed
        from (
            select avg(cast(&target. as double)) as global_avg
            from &data.
            where Muestra='TRAIN'
        );

        select coalesce(std(avg_target), 0) into :_tgt_std_monthly trimmed
        from casuser._tgt_describe
        where Muestra='TRAIN';
    quit;

    %let _tgt_rd_lower=%sysevalf(&_tgt_global_avg. - 2 * &_tgt_std_monthly.);
    %let _tgt_rd_upper=%sysevalf(&_tgt_global_avg. + 2 * &_tgt_std_monthly.);

    proc fedsql sessref=conn;
        create table casuser._tgt_bandas {options replace=true} as
        select Muestra,
            &byvar.,
            N,
            avg_target,
            &_tgt_global_avg. as global_avg,
            &_tgt_rd_lower. as lower_band,
            &_tgt_rd_upper. as upper_band
        from casuser._tgt_describe;
    quit;
%mend _target_build_rd_bands;

%macro _target_build_weighted_avg(data=casuser._tgt_base, target=, monto=, byvar=);
    %global _tgt_global_pond_mean _tgt_pond_lower _tgt_pond_upper;
    %local _tgt_std_monthly;
    %let _tgt_global_pond_mean=.;
    %let _tgt_pond_lower=.;
    %let _tgt_pond_upper=.;
    %let _tgt_std_monthly=.;

    proc fedsql sessref=conn;
        create table casuser._tgt_pond_prom {options replace=true} as
        select Muestra,
            &byvar.,
            sum(cast(&target. as double) * cast(&monto. as double)) /
                sum(cast(&monto. as double)) as avg_target_pond
        from &data.
        where &monto. > 0
        group by Muestra, &byvar.;
    quit;

    proc sql noprint;
        select global_mean into :_tgt_global_pond_mean trimmed
        from (
            select sum(cast(&target. as double) * cast(&monto. as double)) /
                sum(cast(&monto. as double)) as global_mean
            from &data.
            where Muestra='TRAIN' and &monto. > 0
        );

        select coalesce(std(avg_target_pond), 0) into :_tgt_std_monthly trimmed
        from casuser._tgt_pond_prom
        where Muestra='TRAIN';
    quit;

    %let _tgt_pond_lower=%sysevalf(&_tgt_global_pond_mean. - 2 * &_tgt_std_monthly.);
    %let _tgt_pond_upper=%sysevalf(&_tgt_global_pond_mean. + 2 * &_tgt_std_monthly.);

    proc fedsql sessref=conn;
        create table casuser._tgt_pond_prom_bandas {options replace=true} as
        select Muestra,
            &byvar.,
            avg_target_pond,
            &_tgt_global_pond_mean. as global_mean,
            &_tgt_pond_lower. as lower_band,
            &_tgt_pond_upper. as upper_band
        from casuser._tgt_pond_prom;
    quit;
%mend _target_build_weighted_avg;

%macro _target_build_weighted_sum(data=casuser._tgt_base, target=, monto=, byvar=);
    %global _tgt_global_sum_mean _tgt_global_ratio_mean _tgt_sum_lower
        _tgt_sum_upper _tgt_ratio_lower _tgt_ratio_upper;
    %local _tgt_std_sum _tgt_std_ratio;
    %let _tgt_global_sum_mean=.;
    %let _tgt_global_ratio_mean=.;
    %let _tgt_sum_lower=.;
    %let _tgt_sum_upper=.;
    %let _tgt_ratio_lower=.;
    %let _tgt_ratio_upper=.;
    %let _tgt_std_sum=.;
    %let _tgt_std_ratio=.;

    proc fedsql sessref=conn;
        create table casuser._tgt_sum_pond {options replace=true} as
        select Muestra,
            &byvar.,
            sum(cast(&target. as double) * cast(&monto. as double)) as sum_target_pond,
            sum(cast(&monto. as double)) as total_monto
        from &data.
        where &monto. is not null
        group by Muestra, &byvar.;

        create table casuser._tgt_ratio {options replace=true} as
        select Muestra,
            &byvar.,
            case
                when total_monto > 0 then sum_target_pond / total_monto
                else null
            end as ratio_default_monto
        from casuser._tgt_sum_pond;
    quit;

    proc sql noprint;
        select global_mean into :_tgt_global_sum_mean trimmed
        from (
            select avg(sum_target_pond) as global_mean
            from casuser._tgt_sum_pond
            where Muestra='TRAIN'
        );

        select coalesce(std(sum_target_pond), 0) into :_tgt_std_sum trimmed
        from casuser._tgt_sum_pond
        where Muestra='TRAIN';

        select global_mean into :_tgt_global_ratio_mean trimmed
        from (
            select avg(ratio_default_monto) as global_mean
            from casuser._tgt_ratio
            where Muestra='TRAIN' and ratio_default_monto is not null
        );

        select coalesce(std(ratio_default_monto), 0) into :_tgt_std_ratio trimmed
        from casuser._tgt_ratio
        where Muestra='TRAIN' and ratio_default_monto is not null;
    quit;

    %let _tgt_sum_lower=%sysevalf(&_tgt_global_sum_mean. - 2 * &_tgt_std_sum.);
    %let _tgt_sum_upper=%sysevalf(&_tgt_global_sum_mean. + 2 * &_tgt_std_sum.);
    %let _tgt_ratio_lower=%sysevalf(&_tgt_global_ratio_mean. - 2 * &_tgt_std_ratio.);
    %let _tgt_ratio_upper=%sysevalf(&_tgt_global_ratio_mean. + 2 * &_tgt_std_ratio.);

    proc fedsql sessref=conn;
        create table casuser._tgt_sum_pond_bandas {options replace=true} as
        select Muestra,
            &byvar.,
            sum_target_pond,
            total_monto,
            &_tgt_global_sum_mean. as global_mean,
            &_tgt_sum_lower. as lower_band,
            &_tgt_sum_upper. as upper_band
        from casuser._tgt_sum_pond;

        create table casuser._tgt_ratio_bandas {options replace=true} as
        select Muestra,
            &byvar.,
            ratio_default_monto,
            &_tgt_global_ratio_mean. as global_mean,
            &_tgt_ratio_lower. as lower_band,
            &_tgt_ratio_upper. as upper_band
        from casuser._tgt_ratio
        where ratio_default_monto is not null;
    quit;
%mend _target_build_weighted_sum;

%macro _target_compute(input_caslib=, train_table=, oot_table=, byvar=,
    target=, monto_var=, def_cld=0, has_monto=0);

    %global _tgt_global_avg _tgt_global_pond_mean _tgt_global_sum_mean
        _tgt_global_ratio_mean;
    %let _tgt_global_avg=.;
    %let _tgt_global_pond_mean=.;
    %let _tgt_global_sum_mean=.;
    %let _tgt_global_ratio_mean=.;

    %_target_build_base(train_data=&input_caslib..&train_table.,
        oot_data=&input_caslib..&oot_table., byvar=&byvar., def_cld=&def_cld.);
    %_target_build_describe(data=casuser._tgt_base, target=&target.,
        byvar=&byvar.);
    %_target_build_diff_relative(byvar=&byvar.);
    %_target_build_rd_bands(data=casuser._tgt_base, target=&target.,
        byvar=&byvar.);

    %if &has_monto.=1 and %length(%superq(monto_var)) > 0 %then %do;
        %_target_build_weighted_avg(data=casuser._tgt_base, target=&target.,
            monto=&monto_var., byvar=&byvar.);
        %_target_build_weighted_sum(data=casuser._tgt_base, target=&target.,
            monto=&monto_var., byvar=&byvar.);
    %end;
%mend _target_compute;
