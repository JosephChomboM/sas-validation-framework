/* =========================================================================
gini_model_compute.sas - Gini del modelo (general y mensual)
========================================================================= */

%macro _gini_model_general_split(data=, split=, target=, score=,
    with_missing=1, model_low=, model_high=, out=casuser._gini_model_split);

    %local _gini_n _smdcr _gini_split_label;
    %let _gini_split_label=%upcase(%superq(split));

    proc fedsql sessref=conn;
        create table casuser._gini_model_agg {options replace=true} as
        select count(*) as N_Total,
            sum(&target.) as N_Default,
            sum(1-&target.) as N_No_Default,
            (sum(&target.) / count(*)) as Tasa_Default
        from &data.;
    quit;

    %_gini_count_rows(data=&data., target=&target., score=&score.,
        with_missing=&with_missing., outvar=_gini_n);
    %let _smdcr=.;

    %_gini_freqtab_general(data=&data., target=&target., score=&score.,
        with_missing=&with_missing., out=work._gini_model_ft);

    %if %sysfunc(exist(work._gini_model_ft)) %then %do;
        proc sql noprint;
            select max(_SMDCR_) into :_smdcr trimmed from work._gini_model_ft;
        quit;
    %end;

    data &out.;
        length Split $5 Evaluacion $15;
        set casuser._gini_model_agg;
        Split=symget('_gini_split_label');
        N_Gini=input(symget('_gini_n'), best32.);
        Smdcr_Raw=input(symget('_smdcr'), best32.);
        Gini=abs(Smdcr_Raw);
        if N_Gini > 0 and not missing(Gini) then do;
            SE=sqrt(Gini * (1 - Gini) / N_Gini);
            IC_95_Lower=max(0, Gini - 1.96 * SE);
            IC_95_Upper=min(1, Gini + 1.96 * SE);
        end;
        if missing(Gini) then Evaluacion="SIN DATOS";
        else if Gini >= &model_high. then Evaluacion="SATISFACTORIO";
        else if Gini >= &model_low. then Evaluacion="ACEPTABLE";
        else Evaluacion="BAJO";
        format Tasa_Default percent8.2 Gini Smdcr_Raw SE IC_95_Lower
            IC_95_Upper 8.4;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_model_agg;
    quit;

    proc datasets library=work nolist nowarn;
        delete _gini_model_ft;
    quit;

%mend _gini_model_general_split;

%macro _gini_model_general(train_data=, oot_data=, target=, score=,
    with_missing=1, model_low=, model_high=, out=casuser._gini_model_general);

    %_gini_model_general_split(data=&train_data., split=TRAIN, target=&target.,
        score=&score., with_missing=&with_missing., model_low=&model_low.,
        model_high=&model_high., out=casuser._gini_model_train);

    %_gini_model_general_split(data=&oot_data., split=OOT, target=&target.,
        score=&score., with_missing=&with_missing., model_low=&model_low.,
        model_high=&model_high., out=casuser._gini_model_oot);

    data &out.;
        set casuser._gini_model_train casuser._gini_model_oot;
    run;

    %local _gini_train;
    %let _gini_train=.;
    proc sql noprint;
        select Gini into :_gini_train trimmed from &out. where Split="TRAIN";
    quit;

    data &out.;
        set &out.;
        if Split="OOT" and not missing(Gini) and input(symget('_gini_train'),
            best32.) > 0 then Degradacion=
            (input(symget('_gini_train'), best32.) - Gini) /
            input(symget('_gini_train'), best32.);
        format Degradacion percent8.2;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_model_train _gini_model_oot;
    quit;

%mend _gini_model_general;

%macro _gini_model_monthly_split(data=, split=, target=, score=, byvar=,
    with_missing=1, model_low=, model_high=, out=casuser._gini_model_month_split);

    %local _gini_split_label;
    %let _gini_split_label=%upcase(%superq(split));

    data work._gini_model_src;
        set &data.;
    run;

    proc sort data=work._gini_model_src;
        by &byvar.;
    run;

    proc sql noprint;
        create table work._gini_model_cnt as
        select &byvar. as Periodo,
            count(*) as N_Total,
            sum(&target.) as N_Default,
            sum(1-&target.) as N_No_Default,
            calculated N_Default / calculated N_Total as Tasa_Default
                format=percent8.2
        from work._gini_model_src
        group by &byvar.;
    quit;

    %if &with_missing.=1 %then %do;
        proc sql noprint;
            create table work._gini_model_ng as
            select &byvar. as Periodo,
                count(*) as N_Gini
            from work._gini_model_src
            where not missing(&target.)
            group by &byvar.;
        quit;

        proc freqtab data=work._gini_model_src noprint missing;
            by &byvar.;
            tables &target. * &score. / measures;
            output out=work._gini_model_ftb smdcr;
        run;
    %end;
    %else %do;
        proc sql noprint;
            create table work._gini_model_ng as
            select &byvar. as Periodo,
                count(*) as N_Gini
            from work._gini_model_src
            where not missing(&target.) and not missing(&score.)
            group by &byvar.;
        quit;

        proc freqtab data=work._gini_model_src noprint;
            by &byvar.;
            tables &target. * &score. / measures;
            output out=work._gini_model_ftb smdcr;
        run;
    %end;

    proc sql noprint;
        create table work._gini_model_join as
        select c.Periodo,
            c.N_Total,
            c.N_Default,
            c.N_No_Default,
            c.Tasa_Default,
            n.N_Gini,
            f._SMDCR_ as Smdcr_Raw format=8.4,
            abs(f._SMDCR_) as Gini format=8.4
        from work._gini_model_cnt c
        left join work._gini_model_ng n
            on c.Periodo=n.Periodo
        left join work._gini_model_ftb f
            on c.Periodo=f.&byvar.;
    quit;

    data &out.;
        length Split $5 Evaluacion $15;
        set work._gini_model_join;
        Split=symget('_gini_split_label');
        if missing(Gini) then Evaluacion="SIN DATOS";
        else if Gini >= &model_high. then Evaluacion="SATISFACTORIO";
        else if Gini >= &model_low. then Evaluacion="ACEPTABLE";
        else Evaluacion="BAJO";
        format Tasa_Default percent8.2 Gini Smdcr_Raw 8.4;
    run;

    proc datasets library=work nolist nowarn;
        delete _gini_model_src _gini_model_cnt _gini_model_ng _gini_model_ftb
            _gini_model_join;
    quit;

%mend _gini_model_monthly_split;

%macro _gini_model_monthly(train_data=, oot_data=, target=, score=, byvar=,
    with_missing=1, model_low=, model_high=, trend_delta=0.03,
    out=casuser._gini_model_monthly);

    %_gini_model_monthly_split(data=&train_data., split=TRAIN, target=&target.,
        score=&score., byvar=&byvar., with_missing=&with_missing.,
        model_low=&model_low., model_high=&model_high.,
        out=casuser._gini_model_train_m);

    %_gini_model_monthly_split(data=&oot_data., split=OOT, target=&target.,
        score=&score., byvar=&byvar., with_missing=&with_missing.,
        model_low=&model_low., model_high=&model_high.,
        out=casuser._gini_model_oot_m);

    data casuser._gini_model_monthly_all;
        set casuser._gini_model_train_m casuser._gini_model_oot_m;
    run;

    proc fedsql sessref=conn;
        create table casuser._gini_model_fl {options replace=true} as
        select Split,
            min(Periodo) as First_Period,
            max(Periodo) as Last_Period
        from casuser._gini_model_monthly_all
        group by Split;

        create table casuser._gini_model_trend {options replace=true} as
        select a.Split,
            f.Gini as Gini_First,
            l.Gini as Gini_Last,
            (l.Gini - f.Gini) as Delta_Gini
        from casuser._gini_model_fl a
        left join casuser._gini_model_monthly_all f
            on a.Split=f.Split and a.First_Period=f.Periodo
        left join casuser._gini_model_monthly_all l
            on a.Split=l.Split and a.Last_Period=l.Periodo;
    quit;

    proc sql noprint;
        create table work._gini_model_monthly_out as
        select a.*,
            t.Gini_First,
            t.Gini_Last,
            t.Delta_Gini
        from casuser._gini_model_monthly_all a
        left join casuser._gini_model_trend t
            on a.Split=t.Split;
    quit;

    data &out.;
        set work._gini_model_monthly_out;
        length Tendencia $15;
        if missing(Gini_First) or missing(Gini_Last) then Tendencia="SIN DATOS";
        else if Delta_Gini < -&trend_delta. then Tendencia="EMPEORANDO";
        else if Delta_Gini > &trend_delta. then Tendencia="MEJORANDO";
        else Tendencia="ESTABLE";
        format Gini_First Gini_Last Delta_Gini 8.4;
    run;

    proc datasets library=casuser nolist nowarn;
        delete _gini_model_train_m _gini_model_oot_m _gini_model_monthly_all
            _gini_model_fl _gini_model_trend;
    quit;

    proc datasets library=work nolist nowarn;
        delete _gini_model_monthly_out;
    quit;

%mend _gini_model_monthly;
