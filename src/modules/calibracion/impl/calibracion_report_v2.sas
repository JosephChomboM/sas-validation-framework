/* =========================================================================
calibracion_report_v2.sas - Reporte dinamico por Split y Calc_Mode
========================================================================= */

%macro _cal_report_detail(data=, split=, calc_mode=, weighted=0);
    %local _cal_rows;
    %let _cal_rows=0;

    proc sql noprint;
        select count(*) into :_cal_rows trimmed
        from &data.
        where Split="&split." and Calc_Mode="&calc_mode.";
    quit;

    %if &_cal_rows.=0 %then %return;

    title "Calibracion &calc_mode. - &split.";
    proc report data=&data.(where=(Split="&split." and Calc_Mode="&calc_mode."))
        nowd missing;
        columns Var_Seq Variable Bucket_Order Bucket_Label N_Cuentas
            Pct_Cuentas Registros_RD Registros_PD
            %if &weighted.=1 %then %do;
                Registros_RD_Pond Registros_PD_Pond
            %end;
            RD PD LI_10 LS_10 LI_25 LS_25;

        define Var_Seq / order order=data noprint;
        define Variable / display "Variable";
        define Bucket_Order / order order=data noprint;
        define Bucket_Label / display "Bucket" flow width=48
            style(column)=[cellwidth=3.6in just=l];
        define N_Cuentas / display "N Cuentas";
        define Pct_Cuentas / display "% Cuentas" format=percent8.2;
        define Registros_RD / display "Registros RD";
        define Registros_PD / display "Registros PD";
        %if &weighted.=1 %then %do;
            define Registros_RD_Pond / display "Registros RD Pond";
            define Registros_PD_Pond / display "Registros PD Pond";
        %end;
        define RD / display "RD" format=percent8.2;
        define PD / display "PD" format=percent8.2;
        define LI_10 / display "LI 10%" format=percent8.2;
        define LS_10 / display "LS 10%" format=percent8.2;
        define LI_25 / display "LI 25%" format=percent8.2;
        define LS_25 / display "LS 25%" format=percent8.2;
    run;
    title;
%mend _cal_report_detail;

%macro _cal_report_cuts(data=);
    %local _cal_rows;
    %let _cal_rows=0;

    proc sql noprint;
        select count(*) into :_cal_rows trimmed from &data.;
    quit;

    %if &_cal_rows.=0 %then %return;

    title "Cortes numericos de referencia";
    proc report data=&data. nowd missing;
        columns Var_Seq Variable Source_Split Bucket_Order Bucket_Label Inicio Fin;
        define Var_Seq / order order=data noprint;
        define Variable / display "Variable";
        define Source_Split / display "Source Split";
        define Bucket_Order / order order=data noprint;
        define Bucket_Label / display "Bucket" flow width=48
            style(column)=[cellwidth=3.6in just=l];
        define Inicio / display "Inicio" format=best12.4;
        define Fin / display "Fin" format=best12.4;
    run;
    title;
%mend _cal_report_cuts;

%macro _cal_plot_one(detail=, var_seq=, split=, calc_mode=, file_prefix=);
    %local _plot_var _plot_name _plot_rows _plot_has_pd _plot_has_rd
        _plot_has_10 _plot_has_25;
    %let _plot_var=;
    %let _plot_rows=0;
    %let _plot_has_pd=0;
    %let _plot_has_rd=0;
    %let _plot_has_10=0;
    %let _plot_has_25=0;

    proc sql noprint;
        select max(Variable),
               count(*),
               sum(case when not missing(PD) then 1 else 0 end),
               sum(case when not missing(RD) then 1 else 0 end),
               sum(case when not missing(LI_10) and not missing(LS_10)
                   then 1 else 0 end),
               sum(case when not missing(LI_25) and not missing(LS_25)
                   then 1 else 0 end)
        into :_plot_var trimmed,
             :_plot_rows trimmed,
             :_plot_has_pd trimmed,
             :_plot_has_rd trimmed,
             :_plot_has_10 trimmed,
             :_plot_has_25 trimmed
        from &detail.
        where Var_Seq=&var_seq.
          and Split="&split."
          and Calc_Mode="&calc_mode.";
    quit;

    %if &_plot_rows.=0 %then %return;

    %let _plot_name=&file_prefix._v%sysfunc(putn(&var_seq., z3.))_%lowcase(&split.)_%lowcase(&calc_mode.);

    ods graphics / imagename="&_plot_name." imagefmt=jpeg;
    title "Calibracion &calc_mode. - &split. - &_plot_var.";
    proc sgplot data=&detail.(where=(Var_Seq=&var_seq. and Split="&split."
        and Calc_Mode="&calc_mode.")) noautolegend;
        needle x=Bucket_Label y=Pct_Cuentas /
            lineattrs=(color=lightsteelblue thickness=15);
        %if &_plot_has_10. > 0 %then %do;
            band x=Bucket_Label lower=LI_10 upper=LS_10 /
                fillattrs=(color=gold) y2axis;
        %end;
        %if &_plot_has_25. > 0 %then %do;
            band x=Bucket_Label lower=LI_25 upper=LS_25 /
                fillattrs=(color=big) y2axis;
        %end;
        %if &_plot_has_pd. > 0 %then %do;
            series x=Bucket_Label y=PD /
                lineattrs=(color=black thickness=1 pattern=dash) y2axis;
        %end;
        %if &_plot_has_rd. > 0 %then %do;
            scatter x=Bucket_Label y=RD /
                markerattrs=(size=8 symbol=circlefilled color=blue) y2axis;
        %end;
        yaxis label="% Cuentas";
        %if &_plot_has_10. > 0 or &_plot_has_25. > 0 or &_plot_has_pd. > 0
            or &_plot_has_rd. > 0 %then %do;
            y2axis label="PD / RD" min=0;
        %end;
        xaxis label="Buckets driver" valueattrs=(size=7) fitpolicy=rotate
            type=discrete;
    run;
    title;
    ods graphics / reset=all;
%mend _cal_plot_one;

%macro _cal_render_plots(detail=, cuts=, file_prefix=);
    %local _cal_nplots _i;
    %let _cal_nplots=0;

    %_cal_report_cuts(data=&cuts.);

    proc sort data=&detail.(keep=Var_Seq Split Calc_Mode)
        out=work._cal_plot_keys nodupkey;
        by Var_Seq Split Calc_Mode;
    run;

    data _null_;
        set work._cal_plot_keys end=eof;
        call symputx(cats("_cal_plot_seq", _n_), Var_Seq, "L");
        call symputx(cats("_cal_plot_split", _n_), Split, "L");
        call symputx(cats("_cal_plot_mode", _n_), Calc_Mode, "L");
        if eof then call symputx("_cal_nplots", _n_, "L");
    run;

    %if %length(%superq(_cal_nplots))=0 %then %let _cal_nplots=0;

    %do _i=1 %to &_cal_nplots.;
        %_cal_plot_one(detail=&detail., var_seq=&&_cal_plot_seq&_i.,
            split=&&_cal_plot_split&_i., calc_mode=&&_cal_plot_mode&_i.,
            file_prefix=&file_prefix.);
    %end;

    proc datasets library=work nolist nowarn;
        delete _cal_plot_keys;
    quit;
%mend _cal_render_plots;

%macro _cal_sheet_name(split=, calc_mode=, outvar=_cal_sheet_name);
    data _null_;
        length _sheet $31;
        _sheet=substr(cats(upcase("&calc_mode."), "_", upcase("&split.")), 1, 31);
        call symputx("&outvar.", _sheet, "L");
    run;
%mend _cal_sheet_name;

%macro _calibracion_report(detail_data=, cuts_data=, report_path=,
    images_path=, file_prefix=, weighted=0);

    %local _cal_nsheets _i _cur_split _cur_mode _cur_weighted _sheet_name;

    data work._cal_detail_rpt;
        set &detail_data.;
    run;

    data work._cal_cuts_rpt;
        set &cuts_data.;
    run;

    proc sort data=work._cal_detail_rpt(keep=Split Calc_Mode)
        out=work._cal_sheet_keys nodupkey;
        by Calc_Mode Split;
    run;

    %let _cal_nsheets=0;
    data _null_;
        set work._cal_sheet_keys end=eof;
        call symputx(cats("_cal_sheet_split", _n_), Split, "L");
        call symputx(cats("_cal_sheet_mode", _n_), Calc_Mode, "L");
        if eof then call symputx("_cal_nsheets", _n_, "L");
    run;

    ods graphics on;
    ods listing gpath="&images_path.";
    ods html5 file="&report_path./&file_prefix..html"
        options(bitmap_mode="inline");

    %if &_cal_nsheets. > 0 %then %do;
        %let _cur_split=&&_cal_sheet_split1.;
        %let _cur_mode=&&_cal_sheet_mode1.;
        %_cal_sheet_name(split=&_cur_split., calc_mode=&_cur_mode.,
            outvar=_sheet_name);

        ods excel file="&report_path./&file_prefix..xlsx"
            options(sheet_name="&_sheet_name." sheet_interval="none"
            embedded_titles="yes" frozen_headers="yes" autofilter="all");

        %do _i=1 %to &_cal_nsheets.;
            %let _cur_split=&&_cal_sheet_split&_i.;
            %let _cur_mode=&&_cal_sheet_mode&_i.;
            %let _cur_weighted=0;
            %if &weighted.=1 and %upcase(&_cur_mode.)=WGT %then
                %let _cur_weighted=1;
            %_cal_sheet_name(split=&_cur_split., calc_mode=&_cur_mode.,
                outvar=_sheet_name);

            %if &_i. > 1 %then %do;
                ods excel options(sheet_name="&_sheet_name." sheet_interval="now"
                    embedded_titles="yes" frozen_headers="yes" autofilter="all");
            %end;

            %_cal_report_detail(data=work._cal_detail_rpt, split=&_cur_split.,
                calc_mode=&_cur_mode., weighted=&_cur_weighted.);
        %end;
    %end;
    %else %do;
        ods excel file="&report_path./&file_prefix..xlsx"
            options(sheet_name="DETAIL" sheet_interval="none"
            embedded_titles="yes" frozen_headers="yes" autofilter="all");
        title "Calibracion - detalle vacio";
        proc print data=work._cal_detail_rpt noobs;
        run;
        title;
    %end;

    ods excel options(sheet_name="PLOTS" sheet_interval="now"
        embedded_titles="yes");
    %_cal_render_plots(detail=work._cal_detail_rpt, cuts=work._cal_cuts_rpt,
        file_prefix=&file_prefix.);

    ods excel close;
    ods html5 close;
    ods graphics / reset=all;
    ods graphics off;

    proc datasets library=work nolist nowarn;
        delete _cal_detail_rpt _cal_cuts_rpt _cal_sheet_keys;
    quit;
%mend _calibracion_report;
