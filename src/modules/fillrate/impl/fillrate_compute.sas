/* =========================================================================
fillrate_compute.sas - Computo para Fillrate vs Gini

Macros:
- %_fill_general_compute : fillrate general + Gini (_SMDCR_) por variable
- %_fill_monthly_compute : fillrate mensual por variable
- %_fill_plot_monthly    : graficos de tendencia mensual

Notas:
- El Gini usa PROC FREQTAB sin MISSING, segun la metodologia vigente.
- Fillrate numerico trata dummies como no llenados.
- Fillrate categorico usa not missing().
========================================================================= */

%macro _fill_valid_num_expr(var=);
    (&var. not in (., 1111111111, -1111111111, 2222222222, -2222222222,
    3333333333, -3333333333, 4444444444, 5555555555, 6666666666, 7777777777,
    -999999999))
%mend _fill_valid_num_expr;

%macro _fill_general_compute(data=, vars_num=, target=, out=work._fill_general);

    data &out.;
        length Variable $64 Var_Type $8 N_Total N_Filled N_Gini 8 Fillrate
            Gini Smdcr_Raw 8;
        format Fillrate 8.2 Gini 8.4 Smdcr_Raw 8.4;
        stop;
    run;

    %local _i _v _smdcr _n_gini;
    %let _i=1;
    %let _v=%scan(&vars_num., &_i., %str( ));

    %do %while(%length(&_v.) > 0);
        %put NOTE: [fillrate] General + Gini para &_v.;

        proc sql noprint;
            create table work._fill_tmp as
            select "%upcase(&_v.)" as Variable length=64,
                "NUM" as Var_Type length=8,
                count(*) as N_Total,
                sum(case when %_fill_valid_num_expr(var=&_v.) then 1 else 0 end)
                    as N_Filled,
                calculated N_Filled / calculated N_Total * 100 as Fillrate
                    format=8.2
            from &data.;

            select count(*) into :_n_gini trimmed from &data.
                where not missing(&target.) and not missing(&_v.);
        quit;

        %let _smdcr=.;
        proc freqtab data=&data. noprint;
            tables &target. * &_v. / measures;
            output out=work._fill_gini_tmp smdcr;
        run;

        %if %sysfunc(exist(work._fill_gini_tmp)) %then %do;
            proc sql noprint;
                select max(_SMDCR_) into :_smdcr trimmed from work._fill_gini_tmp;
            quit;
        %end;

        data work._fill_tmp2;
            set work._fill_tmp;
            Smdcr_Raw=input(symget('_smdcr'), best32.);
            Gini=abs(Smdcr_Raw);
            N_Gini=input(symget('_n_gini'), best32.);
        run;

        proc append base=&out. data=work._fill_tmp2 force;
        run;

        proc datasets library=work nolist nowarn;
            delete _fill_tmp _fill_tmp2 _fill_gini_tmp;
        quit;

        %let _i=%eval(&_i. + 1);
        %let _v=%scan(&vars_num., &_i., %str( ));
    %end;

%mend _fill_general_compute;

%macro _fill_monthly_compute(data=, byvar=, vars_num=, vars_cat=,
    out=work._fill_monthly);

    data &out.;
        set &data.(obs=0 keep=&byvar.);
        length Variable $64 Var_Type $8 N_Total N_Filled 8 Fillrate 8;
        format Fillrate 8.2;
        stop;
    run;

    %local _i _j _v;

    %let _i=1;
    %let _v=%scan(&vars_num., &_i., %str( ));
    %do %while(%length(&_v.) > 0);
        %put NOTE: [fillrate] Mensual numerica para &_v.;

        proc sql noprint;
            create table work._fill_monthly_tmp as
            select "%upcase(&_v.)" as Variable length=64,
                "NUM" as Var_Type length=8,
                &byvar.,
                count(*) as N_Total,
                sum(case when %_fill_valid_num_expr(var=&_v.) then 1 else 0 end)
                    as N_Filled,
                calculated N_Filled / calculated N_Total * 100 as Fillrate
                    format=8.2
            from &data.
            group by &byvar.;
        quit;

        proc append base=&out. data=work._fill_monthly_tmp force;
        run;

        proc datasets library=work nolist nowarn;
            delete _fill_monthly_tmp;
        quit;

        %let _i=%eval(&_i. + 1);
        %let _v=%scan(&vars_num., &_i., %str( ));
    %end;

    %let _j=1;
    %let _v=%scan(&vars_cat., &_j., %str( ));
    %do %while(%length(&_v.) > 0);
        %put NOTE: [fillrate] Mensual categorica para &_v.;

        proc sql noprint;
            create table work._fill_monthly_tmp as
            select "%upcase(&_v.)" as Variable length=64,
                "CAT" as Var_Type length=8,
                &byvar.,
                count(*) as N_Total,
                sum(case when not missing(&_v.) then 1 else 0 end) as N_Filled,
                calculated N_Filled / calculated N_Total * 100 as Fillrate
                    format=8.2
            from &data.
            group by &byvar.;
        quit;

        proc append base=&out. data=work._fill_monthly_tmp force;
        run;

        proc datasets library=work nolist nowarn;
            delete _fill_monthly_tmp;
        quit;

        %let _j=%eval(&_j. + 1);
        %let _v=%scan(&vars_cat., &_j., %str( ));
    %end;

    proc sort data=&out.;
        by Variable &byvar.;
    run;

%mend _fill_monthly_compute;

%macro _fill_plot_monthly(data=, byvar=, image_stub=);

    %local _nvars _i _var;

    proc sql noprint;
        select count(distinct Variable) into :_nvars trimmed from &data.;
        select distinct Variable into :_fill_plot_v1- from &data.;
    quit;

    %if %length(%superq(_nvars))=0 %then %let _nvars=0;

    %do _i=1 %to &_nvars.;
        %let _var=&&_fill_plot_v&_i.;
        ods graphics / imagename="&image_stub._&_i." imagefmt=jpeg;
        title "Fillrate completo - &_var.";

        proc sgplot data=&data.(where=(Variable="&_var."));
            series x=&byvar. y=Fillrate /
                group=Muestra
                lineattrs=(thickness=2);
            xaxis type=discrete label="&byvar.";
            yaxis label="Fillrate (%)" min=0 max=100;
            keylegend / title="Muestra";
        run;

        title;
    %end;

%mend _fill_plot_monthly;
