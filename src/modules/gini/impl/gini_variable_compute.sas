/* =========================================================================
gini_variable_compute.sas - Gini de variables (general, comparativo y mensual)
========================================================================= */

%macro _gini_variables_general_split(data=, split=, target=, vars_num=,
    with_missing=1, min_n_valid=30, var_low=, var_high=,
    out=casuser._gini_vars_split);

    %local _gini_split_label;
    %let _gini_split_label=%upcase(%superq(split));

    %if %length(%superq(vars_num))=0 %then %do;
        data &out.;
            length Variable $64 Split $5 N_Total N_Valid N_Default N_Gini Ranking 8
                Pct_Valid Gini Smdcr_Raw 8 Evaluacion $15;
            format Pct_Valid percent8.2 Gini Smdcr_Raw 8.4;
            stop;
        run;
        %return;
    %end;

    %_gini_profile_general(data=&data., split=&split., target=&target.,
        vars_num=&vars_num., with_missing=&with_missing.,
        out=casuser._gini_var_profile);

    data work._gini_var_gini;
        length Variable $64 Split $5 Smdcr_Raw Gini 8;
        format Gini Smdcr_Raw 8.4;
        stop;
    run;

    %local _i _var _n_gini_var _smdcr _gini_var_label;
    %let _i=1;
    %let _var=%scan(&vars_num., &_i., %str( ));

    %do %while(%length(&_var.) > 0);
        proc sql noprint;
            select max(N_Gini) into :_n_gini_var trimmed
            from casuser._gini_var_profile
            where Variable="%upcase(&_var.)";
        quit;

        %let _smdcr=.;
        %if %sysfunc(inputn(%superq(_n_gini_var), best32.)) >= &min_n_valid.
            %then %do;
            %_gini_freqtab_general(data=&data., target=&target., score=&_var.,
                with_missing=&with_missing., out=work._gini_var_ft);
            %if %sysfunc(exist(work._gini_var_ft)) %then %do;
                proc sql noprint;
                    select max(_SMDCR_) into :_smdcr trimmed
                    from work._gini_var_ft;
                quit;
            %end;

            %let _gini_var_label=%upcase(%superq(_var));

            data work._gini_var_row;
                length Variable $64 Split $5 Smdcr_Raw Gini 8;
                Variable=symget('_gini_var_label');
                Split=symget('_gini_split_label');
                Smdcr_Raw=input(symget('_smdcr'), best32.);
                Gini=abs(Smdcr_Raw);
                format Gini Smdcr_Raw 8.4;
            run;

            proc append base=work._gini_var_gini data=work._gini_var_row force;
            run;
        %end;

        proc datasets library=work nolist nowarn;
            delete _gini_var_ft _gini_var_row;
        quit;

        %let _i=%eval(&_i. + 1);
        %let _var=%scan(&vars_num., &_i., %str( ));
    %end;

    data casuser._gini_var_gini;
        set work._gini_var_gini;
    run;

    proc fedsql sessref=conn;
        create table casuser._gini_vars_join {options replace=true} as
        select p.Variable,
            p.Split,
            p.N_Total,
            p.N_Valid,
            p.N_Default,
            p.N_Gini,
            g.Smdcr_Raw,
            g.Gini
        from casuser._gini_var_profile p
        left join casuser._gini_var_gini g
            on p.Variable=g.Variable and p.Split=g.Split;
    quit;

    data work._gini_vars_ranked;
        set casuser._gini_vars_join;
        length Evaluacion $15;
        Ranking=.;
        if N_Total > 0 then Pct_Valid=N_Valid / N_Total;
        if N_Gini < &min_n_valid. then Evaluacion="MIN DATOS";
        else if missing(Gini) then Evaluacion="SIN DATOS";
        else if Gini >= &var_high. then Evaluacion="SATISFACTORIO";
        else if Gini >= &var_low. then Evaluacion="ACEPTABLE";
        else Evaluacion="BAJO";
        SortMissing=missing(Gini);
        format Pct_Valid percent8.2 Gini Smdcr_Raw 8.4;
    run;

    proc sort data=work._gini_vars_ranked;
        by SortMissing descending Gini Variable;
    run;

    data &out.;
        set work._gini_vars_ranked;
        retain _rank 0;
        if _n_=1 then _rank=0;
        if SortMissing=0 then do;
            _rank + 1;
            Ranking=_rank;
        end;
        else Ranking=.;
        drop _rank SortMissing;
    run;

    proc datasets library=work nolist nowarn;
        delete _gini_var_ft _gini_var_row _gini_var_gini _gini_vars_ranked;
    quit;

    proc datasets library=casuser nolist nowarn;
        delete _gini_var_profile _gini_var_gini _gini_vars_join;
    quit;

%mend _gini_variables_general_split;

%macro _gini_variables_general(train_data=, oot_data=, target=,
    vars_num_train=, vars_num_oot=, with_missing=1, min_n_valid=30, var_low=,
    var_high=, out=casuser._gini_vars_general);

    %if %length(%superq(vars_num_train))=0 and %length(%superq(vars_num_oot))>0
        %then %let vars_num_train=&vars_num_oot.;
    %if %length(%superq(vars_num_oot))=0 and %length(%superq(vars_num_train))>0
        %then %let vars_num_oot=&vars_num_train.;

    %if %length(%superq(vars_num_train))=0 and %length(%superq(vars_num_oot))=0
        %then %do;
        data &out.;
            length Variable $64 Split $5 N_Total N_Valid N_Default N_Gini Ranking 8
                Pct_Valid Gini Smdcr_Raw 8 Evaluacion $15;
            format Pct_Valid percent8.2 Gini Smdcr_Raw 8.4;
            stop;
        run;
        %return;
    %end;

    %_gini_variables_general_split(data=&train_data., split=TRAIN,
        target=&target., vars_num=&vars_num_train., with_missing=&with_missing.,
        min_n_valid=&min_n_valid., var_low=&var_low., var_high=&var_high.,
        out=casuser._gini_vars_train);

    %_gini_variables_general_split(data=&oot_data., split=OOT,
        target=&target., vars_num=&vars_num_oot., with_missing=&with_missing.,
        min_n_valid=&min_n_valid., var_low=&var_low., var_high=&var_high.,
        out=casuser._gini_vars_oot);

    data &out.;
        set casuser._gini_vars_train casuser._gini_vars_oot;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_vars_train _gini_vars_oot;
    quit;

%mend _gini_variables_general;

%macro _gini_variables_compare(data=, delta_warn=0.05,
    out=casuser._gini_vars_compare);

    proc fedsql sessref=conn;
        create table casuser._gini_vars_compare_raw {options replace=true} as
        select coalesce(t.Variable, o.Variable) as Variable,
            t.Gini as Gini_Train,
            t.Ranking as Rank_Train,
            o.Gini as Gini_OOT,
            o.Ranking as Rank_OOT,
            (t.Gini - o.Gini) as Delta_Gini,
            abs(t.Ranking - o.Ranking) as Delta_Rank
        from (select * from &data. where Split='TRAIN') t
        full join (select * from &data. where Split='OOT') o
            on t.Variable=o.Variable;
    quit;

    data &out.;
        length Variable $64 Estabilidad $15;
        set casuser._gini_vars_compare_raw;
        if missing(Gini_Train) or missing(Gini_OOT) then Estabilidad="SIN DATOS";
        else if Delta_Gini > &delta_warn. then Estabilidad="DEGRADACION";
        else if Delta_Gini < -&delta_warn. then Estabilidad="MEJORA";
        else Estabilidad="ESTABLE";
        format Gini_Train Gini_OOT Delta_Gini 8.4;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_vars_compare_raw;
    quit;

%mend _gini_variables_compare;

%macro _gini_variables_monthly_split(data=, split=, target=, vars_num=,
    byvar=, with_missing=1, min_n_valid=30, var_low=, var_high=,
    out=casuser._gini_vars_month_split);

    %local _gini_split_label;
    %let _gini_split_label=%upcase(%superq(split));

    %if %length(%superq(vars_num))=0 %then %do;
        data &out.;
            length Variable $64 Split $5 Periodo 8 N_Total N_Valid N_Default N_Gini 8
                Gini Smdcr_Raw 8 Evaluacion $15;
            format Gini Smdcr_Raw 8.4;
            stop;
        run;
        %return;
    %end;

    %_gini_profile_monthly(data=&data., split=&split., target=&target.,
        vars_num=&vars_num., byvar=&byvar., with_missing=&with_missing.,
        out=casuser._gini_var_profile_m);

    data work._gini_vars_month_gini;
        length Variable $64 Split $5 Periodo 8 Smdcr_Raw Gini 8;
        format Gini Smdcr_Raw 8.4;
        stop;
    run;

    %local _i _var _gini_var_label _n_gini_var;
    %let _i=1;
    %let _var=%scan(&vars_num., &_i., %str( ));

    %do %while(%length(&_var.) > 0);
        %let _gini_var_label=%upcase(%superq(_var));

        proc sql noprint;
            select max(N_Gini) into :_n_gini_var trimmed
            from casuser._gini_var_profile_m
            where Variable="%upcase(&_var.)";
        quit;

        %if %sysfunc(inputn(%superq(_n_gini_var), best32.)) >= &min_n_valid.
            %then %do;
            %if &with_missing.=1 %then %do;
            proc freqtab data=&data. noprint missing;
                by &byvar.;
                tables &target. * &_var. / measures;
                output out=work._gini_var_ftb smdcr;
            run;
            %end;
            %else %do;
            proc freqtab data=&data. noprint;
                by &byvar.;
                tables &target. * &_var. / measures;
                output out=work._gini_var_ftb smdcr;
            run;
            %end;

            data work._gini_var_row;
            length Variable $64 Split $5 Periodo 8 Smdcr_Raw Gini 8;
            set work._gini_var_ftb;
            Periodo=&byvar.;
            Variable=symget('_gini_var_label');
            Split=symget('_gini_split_label');
            Smdcr_Raw=_SMDCR_;
            Gini=abs(_SMDCR_);
            format Gini Smdcr_Raw 8.4;
            keep Variable Split Periodo Smdcr_Raw Gini;
        run;

        proc append base=work._gini_vars_month_gini data=work._gini_var_row
            force;
        run;
        %end;

        proc datasets library=work nolist nowarn;
            delete _gini_var_ftb _gini_var_row;
        quit;

        %let _i=%eval(&_i. + 1);
        %let _var=%scan(&vars_num., &_i., %str( ));
    %end;

    data casuser._gini_vars_month_gini;
        set work._gini_vars_month_gini;
    run;

    proc fedsql sessref=conn;
        create table casuser._gini_vars_month_join {options replace=true} as
        select p.Variable,
            p.Split,
            p.Periodo,
            p.N_Total,
            p.N_Valid,
            p.N_Default,
            p.N_Gini,
            g.Smdcr_Raw,
            g.Gini
        from casuser._gini_var_profile_m p
        left join casuser._gini_vars_month_gini g
            on p.Variable=g.Variable and p.Split=g.Split and
            p.Periodo=g.Periodo;
    quit;

    data &out.;
        length Evaluacion $15;
        set casuser._gini_vars_month_join;
        if N_Gini < &min_n_valid. then do;
            Smdcr_Raw=.;
            Gini=.;
            Evaluacion="MIN DATOS";
        end;
        else if missing(Gini) then Evaluacion="SIN DATOS";
        else if Gini >= &var_high. then Evaluacion="SATISFACTORIO";
        else if Gini >= &var_low. then Evaluacion="ACEPTABLE";
        else Evaluacion="BAJO";
        format Gini Smdcr_Raw 8.4;
    run;

    proc datasets library=work nolist nowarn;
        delete _gini_var_ftb _gini_var_row _gini_vars_month_gini;
    quit;

    proc datasets library=casuser nolist nowarn;
        delete _gini_var_profile_m _gini_vars_month_gini _gini_vars_month_join;
    quit;

%mend _gini_variables_monthly_split;

%macro _gini_variables_monthly(train_data=, oot_data=, target=,
    vars_num_train=, vars_num_oot=, byvar=, with_missing=1, min_n_valid=30,
    var_low=, var_high=, out=casuser._gini_vars_detail);

    %if %length(%superq(vars_num_train))=0 and %length(%superq(vars_num_oot))>0
        %then %let vars_num_train=&vars_num_oot.;
    %if %length(%superq(vars_num_oot))=0 and %length(%superq(vars_num_train))>0
        %then %let vars_num_oot=&vars_num_train.;

    %if %length(%superq(vars_num_train))=0 and %length(%superq(vars_num_oot))=0
        %then %do;
        data &out.;
            length Variable $64 Split $5 Periodo 8 N_Total N_Valid N_Default N_Gini 8
                Gini Smdcr_Raw 8 Evaluacion $15;
            format Gini Smdcr_Raw 8.4;
            stop;
        run;
        %return;
    %end;

    %_gini_variables_monthly_split(data=&train_data., split=TRAIN,
        target=&target., vars_num=&vars_num_train., byvar=&byvar.,
        with_missing=&with_missing., min_n_valid=&min_n_valid.,
        var_low=&var_low., var_high=&var_high., out=casuser._gini_vars_trn_m);

    %_gini_variables_monthly_split(data=&oot_data., split=OOT,
        target=&target., vars_num=&vars_num_oot., byvar=&byvar.,
        with_missing=&with_missing., min_n_valid=&min_n_valid.,
        var_low=&var_low., var_high=&var_high., out=casuser._gini_vars_oot_m);

    data &out.;
        set casuser._gini_vars_trn_m casuser._gini_vars_oot_m;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_vars_trn_m _gini_vars_oot_m;
    quit;

%mend _gini_variables_monthly;

%macro _gini_variables_summary(data=, var_low=, var_high=, trend_delta=0.03,
    out=casuser._gini_vars_summary);

    proc fedsql sessref=conn;
        create table casuser._gini_vars_base {options replace=true} as
        select Variable,
            Split,
            count(*) as N_Periodos,
            min(Periodo) as First_Period,
            max(Periodo) as Last_Period,
            avg(Gini) as Gini_Promedio,
            min(Gini) as Gini_Min,
            max(Gini) as Gini_Max,
            stddev(Gini) as Gini_Std
        from &data.
        where Gini is not null
        group by Variable, Split;

        create table casuser._gini_vars_summary_raw {options replace=true} as
        select a.Variable,
            a.Split,
            a.N_Periodos,
            a.First_Period,
            a.Last_Period,
            f.Gini as Gini_First,
            l.Gini as Gini_Last,
            a.Gini_Promedio,
            a.Gini_Min,
            a.Gini_Max,
            a.Gini_Std,
            (l.Gini - f.Gini) as Delta_Gini
        from casuser._gini_vars_base a
        left join &data. f
            on a.Variable=f.Variable and a.Split=f.Split and
            a.First_Period=f.Periodo
        left join &data. l
            on a.Variable=l.Variable and a.Split=l.Split and
            a.Last_Period=l.Periodo;
    quit;

    data &out.;
        length Tendencia $15 Evaluacion $15;
        set casuser._gini_vars_summary_raw;
        if missing(Gini_First) or missing(Gini_Last) then Tendencia="SIN DATOS";
        else if Delta_Gini < -&trend_delta. then Tendencia="EMPEORANDO";
        else if Delta_Gini > &trend_delta. then Tendencia="MEJORANDO";
        else Tendencia="ESTABLE";
        if missing(Gini_Promedio) then Evaluacion="SIN DATOS";
        else if Gini_Promedio >= &var_high. then Evaluacion="SATISFACTORIO";
        else if Gini_Promedio >= &var_low. then Evaluacion="ACEPTABLE";
        else Evaluacion="BAJO";
        format Gini_First Gini_Last Gini_Promedio Gini_Min Gini_Max Gini_Std
            Delta_Gini 8.4;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_vars_base _gini_vars_summary_raw;
    quit;

%mend _gini_variables_summary;
