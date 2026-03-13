/* =========================================================================
gini_contract.sas - Validaciones pre-ejecucion del modulo Gini

Verifica:
- TRAIN y OOT accesibles y con observaciones
- target, score, byvar y def_cld definidos
- target, score y byvar existen en ambas tablas
- hay observaciones luego del filtro byvar <= def_cld
- target tiene al menos dos clases luego del filtro
========================================================================= */
%macro gini_contract(input_caslib=, train_table=, oot_table=, target=, score=,
    byvar=, def_cld=);

    %let _gini_rc=0;

    %local _gini_nobs_trn _gini_nobs_oot _gini_has_col _gini_filt_trn
        _gini_filt_oot _gini_levels_trn _gini_levels_oot;

    %if %length(%superq(target))=0 %then %do;
        %put ERROR: [gini_contract] target no definido.;
        %let _gini_rc=1;
        %return;
    %end;

    %if %length(%superq(score))=0 %then %do;
        %put ERROR: [gini_contract] score no definido.;
        %let _gini_rc=1;
        %return;
    %end;

    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [gini_contract] byvar no definido.;
        %let _gini_rc=1;
        %return;
    %end;

    %if %length(%superq(def_cld))=0 %then %do;
        %put ERROR: [gini_contract] def_cld no definido.;
        %let _gini_rc=1;
        %return;
    %end;

    proc sql noprint;
        select count(*) into :_gini_nobs_trn trimmed from
            &input_caslib..&train_table.;
        select count(*) into :_gini_nobs_oot trimmed from
            &input_caslib..&oot_table.;
    quit;

    %if &_gini_nobs_trn.=0 %then %do;
        %put ERROR: [gini_contract] TRAIN no accesible o vacio.;
        %let _gini_rc=1;
        %return;
    %end;

    %if &_gini_nobs_oot.=0 %then %do;
        %put ERROR: [gini_contract] OOT no accesible o vacio.;
        %let _gini_rc=1;
        %return;
    %end;

    %macro _gini_chk_col(mem=, col=, label=);
        %let _gini_has_col=0;
        proc sql noprint;
            select count(*) into :_gini_has_col trimmed from dictionary.columns
                where upcase(libname)=upcase("&input_caslib.") and
                upcase(memname)=upcase("&mem.") and
                upcase(name)=upcase("&col.");
        quit;
        %if &_gini_has_col.=0 %then %do;
            %put ERROR: [gini_contract] &label.=&col. no encontrada en &mem..;
            %let _gini_rc=1;
            %return;
        %end;
    %mend _gini_chk_col;

    %_gini_chk_col(mem=&train_table., col=&target., label=target);
    %if &_gini_rc. ne 0 %then %return;
    %_gini_chk_col(mem=&oot_table., col=&target., label=target);
    %if &_gini_rc. ne 0 %then %return;
    %_gini_chk_col(mem=&train_table., col=&score., label=score);
    %if &_gini_rc. ne 0 %then %return;
    %_gini_chk_col(mem=&oot_table., col=&score., label=score);
    %if &_gini_rc. ne 0 %then %return;
    %_gini_chk_col(mem=&train_table., col=&byvar., label=byvar);
    %if &_gini_rc. ne 0 %then %return;
    %_gini_chk_col(mem=&oot_table., col=&byvar., label=byvar);
    %if &_gini_rc. ne 0 %then %return;

    proc sql noprint;
        select count(*) into :_gini_filt_trn trimmed from
            &input_caslib..&train_table.
            where &byvar. <= &def_cld. and not missing(&target.);
        select count(*) into :_gini_filt_oot trimmed from
            &input_caslib..&oot_table.
            where &byvar. <= &def_cld. and not missing(&target.);
        select count(distinct &target.) into :_gini_levels_trn trimmed from
            &input_caslib..&train_table.
            where &byvar. <= &def_cld. and not missing(&target.);
        select count(distinct &target.) into :_gini_levels_oot trimmed from
            &input_caslib..&oot_table.
            where &byvar. <= &def_cld. and not missing(&target.);
    quit;

    %if &_gini_filt_trn.=0 %then %do;
        %put ERROR: [gini_contract] TRAIN sin observaciones con &byvar. <=
            &def_cld.;
        %let _gini_rc=1;
        %return;
    %end;

    %if &_gini_filt_oot.=0 %then %do;
        %put ERROR: [gini_contract] OOT sin observaciones con &byvar. <=
            &def_cld.;
        %let _gini_rc=1;
        %return;
    %end;

    %if &_gini_levels_trn. < 2 %then %do;
        %put ERROR: [gini_contract] TRAIN no tiene al menos dos clases de
            target luego del filtro temporal.;
        %let _gini_rc=1;
        %return;
    %end;

    %if &_gini_levels_oot. < 2 %then %do;
        %put ERROR: [gini_contract] OOT no tiene al menos dos clases de
            target luego del filtro temporal.;
        %let _gini_rc=1;
        %return;
    %end;

    %put NOTE: [gini_contract] OK - target=&target. score=&score.
        byvar=&byvar. def_cld=&def_cld.;
    %put NOTE: [gini_contract] TRAIN=&_gini_nobs_trn. obs
        (filtrado=&_gini_filt_trn.), OOT=&_gini_nobs_oot. obs
        (filtrado=&_gini_filt_oot.).;

%mend gini_contract;
