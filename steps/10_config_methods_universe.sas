/* =========================================================================
   steps/10_config_methods_universe.sas — Step 10: Métodos para universo
   ========================================================================= */

/* ---- Selección de métodos (tabs) ------------------------------------- */
%let metodo_1_enabled_unv = 1;
%let metodo_1_modules_unv = gini psi;

%let metodo_2_enabled_unv = 0;
%let metodo_2_modules_unv = ;

%let metodo_3_enabled_unv = 0;
%let metodo_3_modules_unv = ;

/* ---- Parámetros por módulo (universo) -------------------------------- */

/* Correlación:
   corr_mode = AUTO   → usa variables de cfg_troncales (num_unv)
                        Outputs van a reports/ y tables/ (validación estándar).
   corr_mode = CUSTOM → usa corr_custom_vars (lista manual de variables numéricas)
                        Outputs van a experiments/ (análisis exploratorio). */
%let corr_mode         = AUTO;
%let corr_custom_vars  = ;

%put NOTE: [step-10] Métodos universo configurados. corr_mode=&corr_mode.;
