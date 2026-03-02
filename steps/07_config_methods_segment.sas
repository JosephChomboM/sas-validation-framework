/* =========================================================================
   steps/07_config_methods_segment.sas — Step 7: Métodos para segmento
   ========================================================================= */

/* ---- Selección de métodos (tabs) ------------------------------------- */
%let metodo_1_enabled_seg = 1;
%let metodo_1_modules_seg = gini psi;

%let metodo_2_enabled_seg = 0;
%let metodo_2_modules_seg = ;

%let metodo_3_enabled_seg = 0;
%let metodo_3_modules_seg = ;

/* ---- Parámetros por módulo (segmento) -------------------------------- */

/* Correlación:
   corr_mode = AUTO   → usa variables de cfg_segmentos/cfg_troncales (num_list/num_unv)
                        Outputs van a reports/ y tables/ (validación estándar).
   corr_mode = CUSTOM → usa corr_custom_vars (lista manual de variables numéricas)
                        Outputs van a experiments/ (análisis exploratorio). */
%let corr_mode         = AUTO;
%let corr_custom_vars  = ;

%put NOTE: [step-07] Métodos segmento configurados. corr_mode=&corr_mode.;
