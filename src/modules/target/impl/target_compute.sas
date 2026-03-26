/* =========================================================================
target_compute.sas - Calculo del modulo Target (Metodo 2.1)

Reconstruye la logica de target_legacy.sas respetando el contrato actual
consumido por target_report.sas. Mantiene la logica simple:
- TRAIN y OOT entran ya derivados por target_run
- Se filtra por &byvar. <= &def_cld.
- Las salidas finales quedan en casuser._tgt_*
- Donde el legacy usaba PROC FREQ, aqui se privilegia PROC FREQTAB
======================================================================== */

%macro _tgt_append(base=, data=);

    %if %sysfunc(exist(&base.)) %then %do;
        proc append base=&base. data=&data. force;
        run;
    %end;
    %else %do;
        data &base.;
            set &data.;
        run;
    %end;

%mend _tgt_append;

%macro _tgt_build_rel_diff(split=, monthly_table=);

    %local _n_months _i _p1 _p2 _p3 _pn_2 _pn_1 _pn _v1 _v2 _v3 _vn_2 _vn_1
        _vn _start_label _end_label _note _window_type _start_value _end_value
        _relative_diff;

    %let _n_months=0;
    proc sql noprint;
        select count(*) into :_n_months trimmed
        from &monthly_table.;

        select strip(put(Periodo, best.)),
               strip(put(RD, best32.))
          into :_tgt_rel_p1-:_tgt_rel_p999,
               :_tgt_rel_v1-:_tgt_rel_v999
        from &monthly_table.
        order by Periodo;
    quit;

    %if %sysevalf(%superq(_n_months)=, boolean) %then %let _n_months=0;
    %if &_n_months. <= 1 %then %goto _tgt_rel_cleanup;

    %if &_n_months. >= 6 %then %do;
        %let _window_type=FIRST3_LAST3;
        %let _p1=&&_tgt_rel_p1.;
        %let _p2=&&_tgt_rel_p2.;
        %let _p3=&&_tgt_rel_p3.;
        %let _pn_2=&&_tgt_rel_p%eval(&_n_months.-2).;
        %let _pn_1=&&_tgt_rel_p%eval(&_n_months.-1).;
        %let _pn=&&_tgt_rel_p&_n_months.;

        %let _v1=&&_tgt_rel_v1.;
        %let _v2=&&_tgt_rel_v2.;
        %let _v3=&&_tgt_rel_v3.;
        %let _vn_2=&&_tgt_rel_v%eval(&_n_months.-2).;
        %let _vn_1=&&_tgt_rel_v%eval(&_n_months.-1).;
        %let _vn=&&_tgt_rel_v&_n_months.;

        %let _start_value=%sysevalf((&_v1. + &_v2. + &_v3.) / 3);
        %let _end_value=%sysevalf((&_vn_2. + &_vn_1. + &_vn.) / 3);
        %let _start_label=Promedio primeros 3 meses (&_p1.-&_p3.);
        %let _end_label=Promedio ultimos 3 meses (&_pn_2.-&_pn.);
        %let _note=Compara promedio de primeros 3 vs ultimos 3 meses.;
    %end;
    %else %do;
        %let _window_type=FIRST_LAST;
        %let _p1=&&_tgt_rel_p1.;
        %let _pn=&&_tgt_rel_p&_n_months.;
        %let _v1=&&_tgt_rel_v1.;
        %let _vn=&&_tgt_rel_v&_n_months.;
        %let _start_value=&_v1.;
        %let _end_value=&_vn.;
        %let _start_label=Primer mes (&_p1.);
        %let _end_label=Ultimo mes (&_pn.);
        %let _note=Compara primer vs ultimo mes.;
    %end;

    %if %sysevalf(&_start_value.=0) %then %do;
        %let _relative_diff=.;
        %let _note=&_note. Referencia inicial igual a cero.;
    %end;
    %else %let _relative_diff=%sysevalf((&_end_value. - &_start_value.) /
        &_start_value.);

    data casuser._tgt_rel_tmp;
        length Split $5 Window_Type $20 Start_Label End_Label $64 Note $120;
        format Start_Value End_Value percent8.4 Relative_Diff percent8.2;
        Split="&split.";
        N_Months=&_n_months.;
        Window_Type="&_window_type.";
        Start_Label="&_start_label.";
        Start_Value=&_start_value.;
        End_Label="&_end_label.";
        End_Value=&_end_value.;
        Relative_Diff=&_relative_diff.;
        Note="&_note.";
        output;
    run;

    %_tgt_append(base=casuser._tgt_rel_diff, data=casuser._tgt_rel_tmp);

    proc datasets library=casuser nolist nowarn;
        delete _tgt_rel_tmp;
    quit;

%_tgt_rel_cleanup:
    %do _i=1 %to 999;
        %if %symexist(_tgt_rel_p&_i.) %then %symdel _tgt_rel_p&_i. / nowarn;
        %if %symexist(_tgt_rel_v&_i.) %then %symdel _tgt_rel_v&_i. / nowarn;
    %end;

%mend _tgt_build_rel_diff;

/* -------------------------------------------------------------------------
Codigo legacy inactivo. Se conserva solo como referencia temporal.
------------------------------------------------------------------------- */
/*
%macro _legacy_tgt_build_materiality(data=, split=, byvar=, target=);

    %local _cnt_var;
    %let _cnt_var=;

    proc freqtab data=&data. noprint;
        tables &byvar. * &target. / norow nopercent nocum nocol;
        output out=casuser._tgt_mat_raw;
    run;

    proc sql noprint;
        select name into :_cnt_var trimmed
        from dictionary.columns
        where upcase(libname)='CASUSER'
          and upcase(memname)='_TGT_MAT_RAW'
          and upcase(name) not in (
              upcase("&byvar."),
              upcase("&target."),
              'PERCENT',
              'ROWPERCENT',
              'COLPERCENT',
              'TABLEPERCENT'
          )
        order by case
                    when upcase(name)='COUNT' then 0
                    when upcase(name)='FREQUENCY' then 1
                    when upcase(name)='N' then 2
                    else 3
                 end,
                 name;
    quit;

    %if %length(%superq(_cnt_var)) = 0 %then %do;
        proc fedsql sessref=conn;
            create table casuser._tgt_mat_tmp {options replace=true} as
            select "&split." as Split,
                   &byvar. as Periodo,
                   &target. as Target_Value,
                   count(*) as N_Cuentas
            from &data.
            where &target. is not missing
            group by &byvar., &target.;
        quit;
    %end;
    %else %do;
        proc fedsql sessref=conn;
            create table casuser._tgt_mat_tmp {options replace=true} as
            select "&split." as Split,
                   &byvar. as Periodo,
                   &target. as Target_Value,
                   &_cnt_var. as N_Cuentas
            from casuser._tgt_mat_raw
            where &byvar. is not missing
              and &target. is not missing;
        quit;
    %end;

    %_tgt_append(base=casuser._tgt_materiality, data=casuser._tgt_mat_tmp);

    proc datasets library=casuser nolist nowarn;
        delete _tgt_mat_raw _tgt_mat_tmp;
    quit;

%mend _legacy_tgt_build_materiality;

%macro _legacy_tgt_build_bands(split=, rd_table=);

    %global _tgt_rd_avg _tgt_rd_std;
    %local _global_avg _std_monthly _inf _sup _min_val _max_val;

    %if %upcase(&split.)=TRAIN or %sysevalf(%superq(_tgt_rd_avg)=, boolean)
        %then %do;
        proc sql noprint;
            select coalesce(mean(RD), 0),
                   coalesce(std(RD), 0)
              into :_global_avg trimmed,
                   :_std_monthly trimmed
            from &rd_table.;
        quit;
        %let _tgt_rd_avg=&_global_avg.;
        %let _tgt_rd_std=&_std_monthly.;
    %end;
    %else %do;
        %let _global_avg=&_tgt_rd_avg.;
        %let _std_monthly=&_tgt_rd_std.;
    %end;

    %let _inf=%sysevalf(&_global_avg. - 2 * &_std_monthly.);
    %let _sup=%sysevalf(&_global_avg. + 2 * &_std_monthly.);
    %let _min_val=%sysevalf(&_global_avg. - 3 * &_std_monthly.);
    %let _max_val=%sysevalf(&_global_avg. + 3 * &_std_monthly.);

    data casuser._tgt_bands_tmp;
        set &rd_table.;
        length Split $5;
        Lower_Band=&_inf.;
        Upper_Band=&_sup.;
        Global_Avg=&_global_avg.;
        Axis_Min=&_min_val.;
        Axis_Max=&_max_val.;
        format RD Lower_Band Upper_Band Global_Avg Axis_Min Axis_Max percent8.4;
    run;

    %_tgt_append(base=casuser._tgt_bands, data=casuser._tgt_bands_tmp);

    proc datasets library=casuser nolist nowarn;
        delete _tgt_bands_tmp;
    quit;

%mend _legacy_tgt_build_bands;

%macro _legacy_tgt_build_weight_avg(data=, split=, byvar=, target=, monto=);

    %global _tgt_wavg_mean _tgt_wavg_std;
    %local _n_rows _global_avg _std_monthly _inf _sup _min_val _max_val;

    %let _n_rows=0;
    proc sql noprint;
        select count(*) into :_n_rows trimmed
        from &data.
        where &monto. > 0;
    quit;

    %if &_n_rows.=0 %then %return;

    proc fedsql sessref=conn;
        create table casuser._tgt_wavg_tmp {options replace=true} as
        select "&split." as Split,
               &byvar. as Periodo,
               count(*) as N_Cuentas,
               sum(&monto.) as Total_Monto,
               sum(&target. * &monto.) / sum(&monto.) as RD_Pond_Prom
        from &data.
        where &monto. > 0
        group by &byvar.;
    quit;

    %if %upcase(&split.)=TRAIN or %sysevalf(%superq(_tgt_wavg_mean)=, boolean)
        %then %do;
        proc sql noprint;
            select coalesce(sum(&target. * &monto.) / sum(&monto.), 0)
              into :_global_avg trimmed
            from &data.
            where &monto. > 0;

            select coalesce(std(RD_Pond_Prom), 0)
              into :_std_monthly trimmed
            from casuser._tgt_wavg_tmp;
        quit;
        %let _tgt_wavg_mean=&_global_avg.;
        %let _tgt_wavg_std=&_std_monthly.;
    %end;
    %else %do;
        %let _global_avg=&_tgt_wavg_mean.;
        %let _std_monthly=&_tgt_wavg_std.;
    %end;

    %let _inf=%sysevalf(&_global_avg. - 2 * &_std_monthly.);
    %let _sup=%sysevalf(&_global_avg. + 2 * &_std_monthly.);
    %let _min_val=%sysevalf(&_global_avg. - 5 * &_std_monthly.);
    %let _max_val=%sysevalf(&_global_avg. + 5 * &_std_monthly.);

    data casuser._tgt_wavg_tmp;
        set casuser._tgt_wavg_tmp;
        Lower_Band=&_inf.;
        Upper_Band=&_sup.;
        Global_Avg=&_global_avg.;
        Axis_Min=&_min_val.;
        Axis_Max=&_max_val.;
        format RD_Pond_Prom Lower_Band Upper_Band Global_Avg Axis_Min Axis_Max
            percent8.6 Total_Monto comma18.2;
    run;

    %_tgt_append(base=casuser._tgt_weight_avg, data=casuser._tgt_wavg_tmp);

    proc datasets library=casuser nolist nowarn;
        delete _tgt_wavg_tmp;
    quit;

%mend _legacy_tgt_build_weight_avg;

%macro _legacy_tgt_build_weight_sum(data=, split=, byvar=, target=, monto=);

    %global _tgt_wsum_mean _tgt_wsum_std _tgt_ratio_mean _tgt_ratio_std;
    %local _global_sum _std_sum _inf _sup _global_ratio _std_ratio _inf_ratio
        _sup_ratio _min_ratio _max_ratio;

    proc fedsql sessref=conn;
        create table casuser._tgt_wsum_tmp {options replace=true} as
        select "&split." as Split,
               &byvar. as Periodo,
               count(*) as N_Cuentas,
               sum(&target. * &monto.) as Sum_Target_Pond,
               sum(&monto.) as Total_Monto
        from &data.
        group by &byvar.;
    quit;

    %if %upcase(&split.)=TRAIN or %sysevalf(%superq(_tgt_wsum_mean)=, boolean)
        %then %do;
        proc sql noprint;
            select coalesce(mean(Sum_Target_Pond), 0),
                   coalesce(std(Sum_Target_Pond), 0)
              into :_global_sum trimmed,
                   :_std_sum trimmed
            from casuser._tgt_wsum_tmp;
        quit;
        %let _tgt_wsum_mean=&_global_sum.;
        %let _tgt_wsum_std=&_std_sum.;
    %end;
    %else %do;
        %let _global_sum=&_tgt_wsum_mean.;
        %let _std_sum=&_tgt_wsum_std.;
    %end;

    %let _inf=%sysevalf(&_global_sum. - 2 * &_std_sum.);
    %let _sup=%sysevalf(&_global_sum. + 2 * &_std_sum.);

    data casuser._tgt_wsum_tmp;
        set casuser._tgt_wsum_tmp;
        Lower_Band=&_inf.;
        Upper_Band=&_sup.;
        Global_Sum=&_global_sum.;
        format Sum_Target_Pond Total_Monto Lower_Band Upper_Band Global_Sum
            comma18.2;
    run;

    %_tgt_append(base=casuser._tgt_weight_sum, data=casuser._tgt_wsum_tmp);

    data casuser._tgt_ratio_tmp;
        set casuser._tgt_wsum_tmp;
        if Total_Monto > 0 then Ratio_RD_Monto=Sum_Target_Pond / Total_Monto;
        else Ratio_RD_Monto=.;
    run;

    %if %upcase(&split.)=TRAIN or
        %sysevalf(%superq(_tgt_ratio_mean)=, boolean) %then %do;
        proc sql noprint;
            select coalesce(mean(Ratio_RD_Monto), 0),
                   coalesce(std(Ratio_RD_Monto), 0)
              into :_global_ratio trimmed,
                   :_std_ratio trimmed
            from casuser._tgt_ratio_tmp;
        quit;
        %let _tgt_ratio_mean=&_global_ratio.;
        %let _tgt_ratio_std=&_std_ratio.;
    %end;
    %else %do;
        %let _global_ratio=&_tgt_ratio_mean.;
        %let _std_ratio=&_tgt_ratio_std.;
    %end;

    %let _inf_ratio=%sysevalf(&_global_ratio. - 2 * &_std_ratio.);
    %let _sup_ratio=%sysevalf(&_global_ratio. + 2 * &_std_ratio.);
    %let _min_ratio=%sysevalf(&_global_ratio. - 5 * &_std_ratio.);
    %let _max_ratio=%sysevalf(&_global_ratio. + 5 * &_std_ratio.);

    data casuser._tgt_ratio_tmp;
        set casuser._tgt_ratio_tmp;
        Lower_Band=&_inf_ratio.;
        Upper_Band=&_sup_ratio.;
        Global_Ratio=&_global_ratio.;
        Axis_Min=&_min_ratio.;
        Axis_Max=&_max_ratio.;
        format Ratio_RD_Monto Lower_Band Upper_Band Global_Ratio Axis_Min
            Axis_Max percent8.6 Sum_Target_Pond Total_Monto comma18.2;
    run;

    %_tgt_append(base=casuser._tgt_weight_ratio, data=casuser._tgt_ratio_tmp);

    proc datasets library=casuser nolist nowarn;
        delete _tgt_wsum_tmp _tgt_ratio_tmp;
    quit;

%mend _legacy_tgt_build_weight_sum;

%macro _legacy_tgt_process_split(data=, split=, byvar=, target=, monto_var=,
    has_monto=0);

    proc fedsql sessref=conn;
        create table casuser._tgt_rd_tmp {options replace=true} as
        select "&split." as Split,
               &byvar. as Periodo,
               count(*) as N_Total,
               count(&target.) as N_Valid,
               sum(&target.) as N_Default,
               avg(&target.) as RD
        from &data.
        group by &byvar.;
    quit;

    %_tgt_append(base=casuser._tgt_rd_monthly, data=casuser._tgt_rd_tmp);
    %_tgt_build_rel_diff(split=&split., monthly_table=casuser._tgt_rd_tmp);
    %_legacy_tgt_build_materiality(data=&data., split=&split., byvar=&byvar.,
        target=&target.);
    %_legacy_tgt_build_bands(split=&split., rd_table=casuser._tgt_rd_tmp);

    %if &has_monto.=1 %then %do;
        %_legacy_tgt_build_weight_avg(data=&data., split=&split., byvar=&byvar.,
            target=&target., monto=&monto_var.);
        %_legacy_tgt_build_weight_sum(data=&data., split=&split., byvar=&byvar.,
            target=&target., monto=&monto_var.);
    %end;

    proc datasets library=casuser nolist nowarn;
        delete _tgt_rd_tmp;
    quit;

%mend _legacy_tgt_process_split;

%macro _legacy_target_compute(input_caslib=, train_table=, oot_table=, byvar=,
    target=, monto_var=, def_cld=, has_monto=0);

    %global _tgt_rd_avg _tgt_rd_std _tgt_wavg_mean _tgt_wavg_std
        _tgt_wsum_mean _tgt_wsum_std _tgt_ratio_mean _tgt_ratio_std;

    proc datasets library=casuser nolist nowarn;
        delete _tgt_:;
    quit;

    proc fedsql sessref=conn;
        create table casuser._tgt_train_base {options replace=true} as
        select *
        from &input_caslib..&train_table.
        where &byvar. <= &def_cld.;

        create table casuser._tgt_oot_base {options replace=true} as
        select *
        from &input_caslib..&oot_table.
        where &byvar. <= &def_cld.;
    quit;

    %_legacy_tgt_process_split(data=casuser._tgt_train_base, split=TRAIN,
        byvar=&byvar., target=&target., monto_var=&monto_var.,
        has_monto=&has_monto.);

    %_legacy_tgt_process_split(data=casuser._tgt_oot_base, split=OOT,
        byvar=&byvar., target=&target., monto_var=&monto_var.,
        has_monto=&has_monto.);

    %if %sysfunc(exist(casuser._tgt_rd_monthly)) %then
        %_tgt_sort_cas(table_name=_tgt_rd_monthly,
            orderby=%str({"Split", "Periodo"}));
    %if %sysfunc(exist(casuser._tgt_rel_diff)) %then
        %_tgt_sort_cas(table_name=_tgt_rel_diff,
            orderby=%str({"Split"}));
    %if %sysfunc(exist(casuser._tgt_materiality)) %then
        %_tgt_sort_cas(table_name=_tgt_materiality,
            orderby=%str({"Split", "Periodo", "Target_Value"}));
    %if %sysfunc(exist(casuser._tgt_bands)) %then
        %_tgt_sort_cas(table_name=_tgt_bands,
            orderby=%str({"Split", "Periodo"}));
    %if %sysfunc(exist(casuser._tgt_weight_avg)) %then
        %_tgt_sort_cas(table_name=_tgt_weight_avg,
            orderby=%str({"Split", "Periodo"}));
    %if %sysfunc(exist(casuser._tgt_weight_sum)) %then
        %_tgt_sort_cas(table_name=_tgt_weight_sum,
            orderby=%str({"Split", "Periodo"}));
    %if %sysfunc(exist(casuser._tgt_weight_ratio)) %then
        %_tgt_sort_cas(table_name=_tgt_weight_ratio,
            orderby=%str({"Split", "Periodo"}));

    proc datasets library=casuser nolist nowarn;
        delete _tgt_train_base _tgt_oot_base;
    quit;

    %symdel _tgt_rd_avg _tgt_rd_std _tgt_wavg_mean _tgt_wavg_std
        _tgt_wsum_mean _tgt_wsum_std _tgt_ratio_mean _tgt_ratio_std / nowarn;

%mend _legacy_target_compute;
*/

/* -------------------------------------------------------------------------
Implementacion activa:
- Usa una sola tabla casuser._tgt_input con columna Split
- Evita sorts y FEDSQL
- Mantiene el contrato que consume target_report.sas
------------------------------------------------------------------------- */

%macro _tgt_detect_freq_count(raw_table=, byvar=, target=, outvar=);

    %local _memname;
    %let _memname=%upcase(%scan(&raw_table., 2, .));
    %if %length(%superq(_memname))=0 %then
        %let _memname=%upcase(&raw_table.);

    proc sql noprint;
        select name into :&outvar trimmed
        from dictionary.columns
        where upcase(libname)='CASUSER'
          and upcase(memname)="&_memname."
          and type='num'
          and upcase(name) not in (
              'SPLIT',
              upcase("&byvar."),
              upcase("&target."),
              'PERCENT',
              'ROWPERCENT',
              'COLPERCENT',
              'TABLEPERCENT'
          )
        order by case
                    when upcase(name)='COUNT' then 0
                    when upcase(name)='FREQUENCY' then 1
                    when upcase(name)='N' then 2
                    else 3
                 end,
                 name;
    quit;

%mend _tgt_detect_freq_count;

%macro _tgt_build_materiality(data=, byvar=, target=);

    %local _cnt_var;
    %let _cnt_var=;

    proc freqtab data=&data. noprint;
        tables &byvar. * &target. / norow nopercent nocum nocol;
        output out=casuser._tgt_mat_raw;
    run;

    %_tgt_detect_freq_count(raw_table=casuser._tgt_mat_raw, byvar=&byvar.,
        target=&target., outvar=_cnt_var);

    %if %length(%superq(_cnt_var)) > 0 %then %do;
        data casuser._tgt_materiality;
            set casuser._tgt_mat_raw(rename=(
                &byvar.=Periodo
                &target.=Target_Value
                &_cnt_var.=N_Cuentas
            ));
            where not missing(Split)
              and not missing(Periodo)
              and not missing(Target_Value);
            keep Split Periodo Target_Value N_Cuentas;
        run;
    %end;
    %else %do;
        proc sql;
            create table casuser._tgt_materiality as
            select Split,
                   &byvar. as Periodo,
                   &target. as Target_Value,
                   count(*) as N_Cuentas
            from &data.
            where &target. is not missing
            group by Split, &byvar., &target.;
        quit;
    %end;

    proc datasets library=casuser nolist nowarn;
        delete _tgt_mat_raw;
    quit;

%mend _tgt_build_materiality;

%macro _tgt_build_bands(rd_table=);

    %local _ref_split _train_rows _global_avg _std_monthly _inf _sup _min_val
        _max_val;
    %let _ref_split=TRAIN;

    proc sql noprint;
        select count(*) into :_train_rows trimmed
        from &rd_table.
        where Split='TRAIN';
    quit;

    %if %sysevalf(%superq(_train_rows)=, boolean) or &_train_rows.=0 %then
        %let _ref_split=OOT;

    proc sql noprint;
        select coalesce(mean(RD), 0),
               coalesce(std(RD), 0)
          into :_global_avg trimmed,
               :_std_monthly trimmed
        from &rd_table.
        where Split="&_ref_split.";
    quit;

    %let _inf=%sysevalf(&_global_avg. - 2 * &_std_monthly.);
    %if %sysevalf(&_inf. < 0) %then %let _inf=0;
    %let _sup=%sysevalf(&_global_avg. + 2 * &_std_monthly.);
    %let _min_val=%sysevalf(&_global_avg. - 3 * &_std_monthly.);
    %let _max_val=%sysevalf(&_global_avg. + 3 * &_std_monthly.);

    data casuser._tgt_bands;
        set &rd_table.;
        Lower_Band=&_inf.;
        Upper_Band=&_sup.;
        Global_Avg=&_global_avg.;
        Axis_Min=&_min_val.;
        Axis_Max=&_max_val.;
        format RD Lower_Band Upper_Band Global_Avg Axis_Min Axis_Max percent8.4;
    run;

%mend _tgt_build_bands;

%macro _tgt_build_weight_avg(data=, byvar=, target=, monto=);

    %local _ref_split _n_rows _train_rows _global_avg _std_monthly _inf _sup
        _min_val _max_val;
    %let _ref_split=TRAIN;

    proc sql noprint;
        select count(*) into :_n_rows trimmed
        from &data.
        where &monto. > 0;
    quit;

    %if %sysevalf(%superq(_n_rows)=, boolean) or &_n_rows.=0 %then %return;

    proc sql;
        create table casuser._tgt_weight_avg as
        select Split,
               &byvar. as Periodo,
               count(*) as N_Cuentas,
               sum(&monto.) as Total_Monto,
               sum(&target. * &monto.) / sum(&monto.) as RD_Pond_Prom
        from &data.
        where &monto. > 0
        group by Split, &byvar.;
    quit;

    proc sql noprint;
        select count(*) into :_train_rows trimmed
        from casuser._tgt_weight_avg
        where Split='TRAIN';
    quit;

    %if %sysevalf(%superq(_train_rows)=, boolean) or &_train_rows.=0 %then
        %let _ref_split=OOT;

    proc sql noprint;
        select coalesce(sum(&target. * &monto.) / sum(&monto.), 0)
          into :_global_avg trimmed
        from &data.
        where Split="&_ref_split."
          and &monto. > 0;

        select coalesce(std(RD_Pond_Prom), 0)
          into :_std_monthly trimmed
        from casuser._tgt_weight_avg
        where Split="&_ref_split.";
    quit;

    %let _inf=%sysevalf(&_global_avg. - 2 * &_std_monthly.);
    %let _sup=%sysevalf(&_global_avg. + 2 * &_std_monthly.);
    %let _min_val=%sysevalf(&_global_avg. - 5 * &_std_monthly.);
    %let _max_val=%sysevalf(&_global_avg. + 5 * &_std_monthly.);

    data casuser._tgt_weight_avg;
        set casuser._tgt_weight_avg;
        Lower_Band=&_inf.;
        Upper_Band=&_sup.;
        Global_Avg=&_global_avg.;
        Axis_Min=&_min_val.;
        Axis_Max=&_max_val.;
        format RD_Pond_Prom Lower_Band Upper_Band Global_Avg Axis_Min Axis_Max
            percent8.6 Total_Monto comma18.2;
    run;

%mend _tgt_build_weight_avg;

%macro _tgt_build_weight_sum(data=, byvar=, target=, monto=);

    %local _ref_split _train_rows _global_sum _std_sum _inf _sup
        _global_ratio _std_ratio _inf_ratio _sup_ratio _min_ratio _max_ratio;
    %let _ref_split=TRAIN;

    proc sql;
        create table casuser._tgt_weight_sum as
        select Split,
               &byvar. as Periodo,
               count(*) as N_Cuentas,
               sum(&target. * &monto.) as Sum_Target_Pond,
               sum(&monto.) as Total_Monto
        from &data.
        group by Split, &byvar.;
    quit;

    proc sql noprint;
        select count(*) into :_train_rows trimmed
        from casuser._tgt_weight_sum
        where Split='TRAIN';
    quit;

    %if %sysevalf(%superq(_train_rows)=, boolean) or &_train_rows.=0 %then
        %let _ref_split=OOT;

    proc sql noprint;
        select coalesce(mean(Sum_Target_Pond), 0),
               coalesce(std(Sum_Target_Pond), 0)
          into :_global_sum trimmed,
               :_std_sum trimmed
        from casuser._tgt_weight_sum
        where Split="&_ref_split.";
    quit;

    %let _inf=%sysevalf(&_global_sum. - 2 * &_std_sum.);
    %let _sup=%sysevalf(&_global_sum. + 2 * &_std_sum.);

    data casuser._tgt_weight_sum;
        set casuser._tgt_weight_sum;
        Lower_Band=&_inf.;
        Upper_Band=&_sup.;
        Global_Sum=&_global_sum.;
        format Sum_Target_Pond Total_Monto Lower_Band Upper_Band Global_Sum
            comma18.2;
    run;

    data casuser._tgt_weight_ratio;
        set casuser._tgt_weight_sum;
        if Total_Monto > 0 then Ratio_RD_Monto=Sum_Target_Pond / Total_Monto;
        else Ratio_RD_Monto=.;
    run;

    proc sql noprint;
        select coalesce(mean(Ratio_RD_Monto), 0),
               coalesce(std(Ratio_RD_Monto), 0)
          into :_global_ratio trimmed,
               :_std_ratio trimmed
        from casuser._tgt_weight_ratio
        where Split="&_ref_split.";
    quit;

    %let _inf_ratio=%sysevalf(&_global_ratio. - 2 * &_std_ratio.);
    %let _sup_ratio=%sysevalf(&_global_ratio. + 2 * &_std_ratio.);
    %let _min_ratio=%sysevalf(&_global_ratio. - 5 * &_std_ratio.);
    %let _max_ratio=%sysevalf(&_global_ratio. + 5 * &_std_ratio.);

    data casuser._tgt_weight_ratio;
        set casuser._tgt_weight_ratio;
        Lower_Band=&_inf_ratio.;
        Upper_Band=&_sup_ratio.;
        Global_Ratio=&_global_ratio.;
        Axis_Min=&_min_ratio.;
        Axis_Max=&_max_ratio.;
        format Ratio_RD_Monto Lower_Band Upper_Band Global_Ratio Axis_Min
            Axis_Max percent8.6 Sum_Target_Pond Total_Monto comma18.2;
    run;

%mend _tgt_build_weight_sum;

%macro _target_compute(input_caslib=, train_table=, oot_table=, byvar=,
    target=, monto_var=, def_cld=, has_monto=0);

    proc datasets library=casuser nolist nowarn;
        delete _tgt_:;
    quit;

    data casuser._tgt_input;
        length Split $5;
        set &input_caslib..&train_table.(in=_train where=(&byvar. <= &def_cld.))
            &input_caslib..&oot_table.(in=_oot where=(&byvar. <= &def_cld.));
        if _train then Split='TRAIN';
        else if _oot then Split='OOT';
    run;

    data casuser._tgt_rel_diff;
        length Split $5 Window_Type $20 Start_Label End_Label $64 Note $120;
        length N_Months Start_Value End_Value Relative_Diff 8;
        format Start_Value End_Value percent8.4 Relative_Diff percent8.2;
        stop;
    run;

    proc sql;
        create table casuser._tgt_rd_monthly as
        select Split,
               &byvar. as Periodo,
               count(*) as N_Total,
               count(&target.) as N_Valid,
               sum(&target.) as N_Default,
               mean(&target.) as RD format=percent8.4
        from casuser._tgt_input
        group by Split, &byvar.;
    quit;

    %_tgt_build_rel_diff(split=TRAIN,
        monthly_table=casuser._tgt_rd_monthly(where=(Split='TRAIN')));
    %_tgt_build_rel_diff(split=OOT,
        monthly_table=casuser._tgt_rd_monthly(where=(Split='OOT')));

    %_tgt_build_materiality(data=casuser._tgt_input, byvar=&byvar.,
        target=&target.);
    %_tgt_build_bands(rd_table=casuser._tgt_rd_monthly);

    %if &has_monto.=1 %then %do;
        %_tgt_build_weight_avg(data=casuser._tgt_input, byvar=&byvar.,
            target=&target., monto=&monto_var.);
        %_tgt_build_weight_sum(data=casuser._tgt_input, byvar=&byvar.,
            target=&target., monto=&monto_var.);
    %end;

    proc datasets library=casuser nolist nowarn;
        delete _tgt_input;
    quit;

%mend _target_compute;
