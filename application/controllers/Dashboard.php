<?php
defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Dashboard extends REST_Controller {
    
    public function __construct(){

        parent::__construct();
        $this->load->helper('json_utilities');
        $this->load->database();
    }   
    
    public function index_get(){
        
        
//        GET DASHBOARD CONFIGURATION
        $cfg_res = $this->db->get_where('config_dashboard', array('tag' => 'main') );
        $configuracion = $cfg_res->row();
        
//        CONFIGURE DATE RELATIONSHIPS
        $x=1;
        for($i=date('Y-m-d', strtotime($configuracion->ly_inicio));$i<=date('Y-m-d', strtotime($configuracion->ly_fin));$i=date('Y-m-d',strtotime($i.' +1 day'))){
          $tmpdate=date('Y-m-d',strtotime($configuracion->td_inicio." +".($x-1)." day"));
          $fecha_int[$i]=$x;
          $fecha_int[$tmpdate]=$x;
          $fecha_json[$x]=date('d-M',strtotime($tmpdate));
          @$radioprint.="<label for='radio-$x'>".date('d-M',strtotime($tmpdate))."</label>
                        <input class='chkbx' type='radio' name='radio-1' id='radio-$x' value=$x>\n ";
          @$plotbands.="{ //$x
                          from: ".(($x-1)*96).",
                          to: ".(($x)*96).",
                          label: {
                              text: '".date('d-M',strtotime($tmpdate))."',
                              align: 'right',
                              x: -10,
                              style: {
                                  color: '#eff3ff'
                              }
                          },
                          borderColor: 'rgba(109, 145, 25, .2)',
                          borderWidth: 4
                      },";
          $x++;
        }

//        BUILD PT CHANNELS
        $canales = $this->db->select('canales')->from('PtChannels')->get();
        $channels = $canales->row_array();

//        BUILD MODULOS PARA GRUPO POR CANAL
        $canales_query = $this->db->query("SELECT dashboard,query FROM monitor_kpiLive_modules WHERE pais='MX' AND dashboard!='MT'");
        foreach($canales_query->result_array() as $row => $data){
           
            $tmptxt=$data['query'];
            @$canales.=str_replace('$ptChannels',$channels['canales'],str_replace("DepOK","dep",str_replace("a.","b.",substr($tmptxt,0,strpos($tmptxt,"'")+1).$data['dashboard']."' ")));
            
        }

        
//        DROP TEMPORARY EXISTING TABLES
        $this->db->query('DROP TEMPORARY TABLE IF EXISTS dash_td');
        $this->db->query('DROP TEMPORARY TABLE IF EXISTS td_dash');
        $this->db->query('DROP TEMPORARY TABLE IF EXISTS td_created');
        $this->db->query('DROP TEMPORARY TABLE IF EXISTS locs_shown');
        $this->db->query('DROP TEMPORARY TABLE IF EXISTS dashboard_venta');
        
        
//        LOCS SHOWN
        $where = "Fecha BETWEEN '".$configuracion->ly_inicio."' AND '".$configuracion->ly_fin."' OR Fecha BETWEEN '".$configuracion->td_inicio."' AND '".$configuracion->td_fin."'";
        $this->db->query("CREATE TEMPORARY TABLE locs_shown ".$this->db->select()->from('t_Locs')
                ->where($where)->get_compiled_select());
        $this->db->query('ALTER TABLE locs_shown
          ADD PRIMARY KEY (Localizador, Venta, Fecha, Hora)');
        
        
//        INSERT TODAY INFO
        $this->db->query("INSERT INTO locs_shown (SELECT * FROM (SELECT * FROM d_Locs WHERE Fecha BETWEEN ADDDATE(CURDATE(),-1) AND CURDATE()) a ) ON DUPLICATE KEY UPDATE asesor=a.asesor");
        
//        TD DASH
        $this->db->query("CREATE TEMPORARY TABLE td_dash SELECT
                          a.*, 
                          IF(Venta!=0,VentaMXN+OtrosIngresosMXN+EgresosMXN,0) as MontoVenta,
                          IF(Venta=0,IF(OtrosIngresosMXN!=0,OtrosIngresosMXN+EgresosMXN,0),0) as MontoOI,
                          IF(Venta=0,IF(OtrosIngresosMXN=0,EgresosMXN,0),0) as MontoEgresos
                        FROM 
                          locs_shown a
                        WHERE 
                          chanId IN (".$channels['canales'].")");
        
        $this->db->query("ALTER TABLE td_dash
          ADD PRIMARY KEY (Localizador, Venta, Fecha, Hora)");
          
        
//        TD CREATED
        $this->db->query("CREATE TEMPORARY TABLE td_created SELECT Fecha, Localizador, IF(Venta!=0,Localizador,NULL) as VentaHoy FROM td_dash WHERE Venta!=0 GROUP BY Fecha, Localizador");

        $this->db->query("ALTER TABLE td_created
          ADD PRIMARY KEY (Fecha, Localizador)");
        
//        DASHBOARD VENTA
        $this->db->query("CREATE TEMPORARY TABLE dashboard_venta SELECT
                        a.*, b.VentaHoy,
                        CASE
                          WHEN b.VentaHoy IS NOT NULL THEN MontoVenta+MontoOI+MontoEgresos
                          ELSE IF(MontoOI>0 OR MontoEgresos>0,MontoVenta+MontoOI+MontoEgresos,0)
                        END as MontoDia
                      FROM
                        td_dash a
                      LEFT JOIN
                        td_created b ON a.Localizador=b.Localizador AND a.Fecha=b.Fecha");

        $this->db->query("ALTER TABLE dashboard_venta
          ADD PRIMARY KEY (Localizador, Venta, Fecha, Hora)");
        
        
            
//        DASH TD
        $this->db->query("CREATE TEMPORARY TABLE dash_td (SELECT Fecha, Hora_int, Hora_pretty, 
        CASE WHEN chanId IN (295,355) THEN 'Outlet' $canales END as OAfiliado, 
        COUNT(DISTINCT VentaHoy) as Locs, SUM(MontoDia) as Monto, SUM(VentaMXN+OtrosIngresosMXN) as SoloVenta 
        FROM 
          HoraGroup_Table15 a 
        RIGHT JOIN 
          (SELECT 
            a.*, 
            AfiliadoOK as Canal, 
            dep 
          FROM 
            dashboard_venta a 
          LEFT JOIN 
            chanIds b ON a.chanId=b.id 
          LEFT JOIN 
            dep_asesores c ON a.asesor=c.asesor AND a.Fecha=c.Fecha 
          ) b 
          ON b.Hora BETWEEN a.Hora_time AND ADDTIME(a.Hora_time,'00:14:59') 
        LEFT JOIN
          chanIds c ON b.chanId=c.id
        GROUP BY 
          Fecha, Hora_pretty, OAfiliado
         ORDER BY
          Fecha, Hora_int)");
        
        $totals = $this->db->select('Fecha, OAfiliado, SUM(Monto) as Monto')->from('dash_td')->where("YEAR(Fecha) AND YEAR(CURDATE())")->group_by(array('Fecha', 'Oafiliado'))->get();
        
        foreach($totals->result_array() as $index => $info){
            
            $fecha=$fecha_int[$info['Fecha']];
            
            $tit_td[$info['OAfiliado']][$fecha]=$info['Monto'];
            @$tit_td['ad'][$info['OAfiliado']]+=$info['Monto'];

            @$tit_td['Total'][$fecha]+=$info['Monto'];
            @$tit_td['ad']['Total']+=$info['Monto'];

            switch($info['OAfiliado']){
              case 'COM':
              case 'CC':
              case 'OB':
                @$tit_td['Total COM'][$fecha]+=$info['Monto'];
                @$tit_td['ad']['Total COM']+=$info['Monto'];
                break;
            }
        }
        
        $tdDash = $this->db->select('*')->from('td_dash')->order_by('YEAR(Fecha)')->get();
        
        jsonPrint($tdDash->result());
        
        
            
        
        
    }
}