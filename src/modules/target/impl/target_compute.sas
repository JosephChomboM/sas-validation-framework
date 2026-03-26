/* =========================================================================
target_compute.sas - Calculo del modulo Target (Metodo 2.1)

Implementacion simple y rapida:
- Input unificado TRAIN + OOT en casuser._tgt_input
- Sin codigo legacy ni comentado
- Sin sort innecesario
- Calculo directo con DATA STEP + SUMMARY/SQL
======================================================================== */

%macro _tgt_append(base=, data=);

    %if %sysfunc(exist(&base.)) %then %do;
        data &base.;
            set &base. &data.;
        run;
    %end;
    %else %do;
        data &base.;
            set &data.;
        run;
    %end;

%mend _tgt_append;

%macro _tgt_build_rel_diff(split=, monthly_table=);

    %local _n_months _p1 _p2 _p3 _pn _lp1 _lp2 _lp3 _v1 _v2 _v3 _vn _lv1
        _lv2 _lv3 _start_label _end_label _note _window_type _start_value
        _end_value _relative_diff;

    %let _n_months=0;
    proc sql noprint;
        select count(*) into :_n_months trimmed
        from &monthly_table.;
    quit;

    %if %sysevalf(%superq(_n_months)=, boolean) %then %let _n_months=0;
    %if &_n_months. <= 1 %then %return;

    %if &_n_months. >= 6 %then %do;
        proc sql noprint outobs=3;
            select strip(put(Periodo, best.)),
                   strip(put(RD, best32.))
              into :_p1-:_p3,
                   :_v1-:_v3
            from &monthly_table.
            order by Periodo;
        quit;

        proc sql noprint outobs=3;
            select strip(put(Periodo, best.)),
                   strip(put(RD, best32.))
              into :_lp1-:_lp3,
                   :_lv1-:_lv3
            from &monthly_table.
            order by Periodo desc;
        quit;

        %let _window_type=FIRST3_LAST3;
        %let _start_value=%sysevalf((&_v1. + &_v2. + &_v3.) / 3);
        %let _end_value=%sysevalf((&_lv1. + &_lv2. + &_lv3.) / 3);
        %let _start_label=Promedio primeros 3 meses (&_p1.-&_p3.);
        %let _end_label=Promedio ultimos 3 meses (&_lp3.-&_lp1.);
        %let _note=Compara promedio de primeros 3 vs ultimos 3 meses.;
    %end;
    %else %do;
        proc sql noprint outobs=1;
            select strip(put(Periodo, best.)),
                   strip(put(RD, best32.))
              into :_p1,
                   :_v1
            from &monthly_table.
            order by Periodo;
        quit;

        proc sql noprint outobs=1;
            select strip(put(Periodo, best.)),
                   strip(put(RD, best32.))
              into :_pn,
                   :_vn
            from &monthly_table.
            order by Periodo desc;
        quit;

        %let _window_type=FIRST_LAST;
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

%mend _tgt_build_rel_diff;

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

    %local _rows _ref_split _train_rows _global_avg _std_monthly _inf _sup
        _min_val _max_val;
    %let _ref_split=TRAIN;

    data casuser._tgt_wavg_base;
        set &data.(where=(&monto. > 0));
        _obs_count=1;
        _target_monto=&target. * &monto.;
    run;

    proc summary data=casuser._tgt_wavg_base nway;
        class Split &byvar.;
        var _obs_count &monto. _target_monto;
        output out=casuser._tgt_weight_avg(drop=_type_ _freq_)
            sum(_obs_count)=N_Cuentas
            sum(&monto.)=Total_Monto
            sum(_target_monto)=_sum_target_monto;
    run;

    data casuser._tgt_weight_avg;
        set casuser._tgt_weight_avg(rename=(&byvar.=Periodo));
        if Total_Monto > 0 then RD_Pond_Prom=_sum_target_monto / Total_Monto;
        else RD_Pond_Prom=.;
        keep Split Periodo N_Cuentas Total_Monto RD_Pond_Prom;
    run;

    proc sql noprint;
        select count(*) into :_rows trimmed
        from casuser._tgt_weight_avg;
    quit;

    %if %sysevalf(%superq(_rows)=, boolean) or &_rows.=0 %then %do;
        proc datasets library=casuser nolist nowarn;
            delete _tgt_wavg_base _tgt_weight_avg;
        quit;
        %return;
    %end;

    proc sql noprint;
        select count(*) into :_train_rows trimmed
        from casuser._tgt_weight_avg
        where Split='TRAIN';
    quit;

    %if %sysevalf(%superq(_train_rows)=, boolean) or &_train_rows.=0 %then
        %let _ref_split=OOT;

    proc sql noprint;
        select coalesce(sum(_target_monto) / sum(&monto.), 0)
          into :_global_avg trimmed
        from casuser._tgt_wavg_base
        where Split="&_ref_split.";

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

    proc datasets library=casuser nolist nowarn;
        delete _tgt_wavg_base;
    quit;

%mend _tgt_build_weight_avg;

%macro _tgt_build_weight_sum(data=, byvar=, target=, monto=);

    %local _rows _ref_split _train_rows _global_sum _std_sum _inf _sup
        _global_ratio _std_ratio _inf_ratio _sup_ratio _min_ratio _max_ratio;
    %let _ref_split=TRAIN;

    data casuser._tgt_wsum_base;
        set &data.;
        _obs_count=1;
        _target_monto=&target. * &monto.;
    run;

    proc summary data=casuser._tgt_wsum_base nway;
        class Split &byvar.;
        var _obs_count _target_monto &monto.;
        output out=casuser._tgt_weight_sum(drop=_type_ _freq_)
            sum(_obs_count)=N_Cuentas
            sum(_target_monto)=Sum_Target_Pond
            sum(&monto.)=Total_Monto;
    run;

    data casuser._tgt_weight_sum;
        set casuser._tgt_weight_sum(rename=(&byvar.=Periodo));
        keep Split Periodo N_Cuentas Sum_Target_Pond Total_Monto;
    run;

    proc sql noprint;
        select count(*) into :_rows trimmed
        from casuser._tgt_weight_sum;
    quit;

    %if %sysevalf(%superq(_rows)=, boolean) or &_rows.=0 %then %do;
        proc datasets library=casuser nolist nowarn;
            delete _tgt_wsum_base _tgt_weight_sum;
        quit;
        %return;
    %end;

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

    proc datasets library=casuser nolist nowarn;
        delete _tgt_wsum_base;
    quit;

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

    proc summary data=casuser._tgt_input nway;
        class Split &byvar.;
        var &target.;
        output out=casuser._tgt_rd_monthly(drop=_type_)
            n(&target.)=N_Valid
            sum(&target.)=N_Default
            mean(&target.)=RD;
    run;

    data casuser._tgt_rd_monthly;
        set casuser._tgt_rd_monthly;
        Periodo=&byvar.;
        N_Total=_freq_;
        keep Split Periodo N_Total N_Valid N_Default RD;
        format RD percent8.4;
    run;

    %_tgt_build_rel_diff(split=TRAIN,
        monthly_table=casuser._tgt_rd_monthly(where=(Split='TRAIN')));
    %_tgt_build_rel_diff(split=OOT,
        monthly_table=casuser._tgt_rd_monthly(where=(Split='OOT')));

    %_tgt_build_bands(rd_table=casuser._tgt_rd_monthly);

    %if &has_monto.=1 %then %do;
        %_tgt_build_weight_avg(data=casuser._tgt_input, byvar=&byvar.,
            target=&target., monto=&monto_var.);
        %_tgt_build_weight_sum(data=casuser._tgt_input, byvar=&byvar.,
            target=&target., monto=&monto_var.);
    %end;

%mend _target_compute;
