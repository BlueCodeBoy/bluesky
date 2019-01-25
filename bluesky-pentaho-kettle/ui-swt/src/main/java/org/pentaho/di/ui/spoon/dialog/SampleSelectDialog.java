/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.ui.spoon.dialog;

import javassist.tools.reflect.Sample;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;


/**
 * Dialog to enter a text. (descriptions etc.)
 *
 * @author Samatar
 * @since 20-04-2009
 */
public class SampleSelectDialog extends Dialog {

  private static Class<?> PKG = SampleSelectDialog.class; // for i18n purposes, needed by Translator2!!
  final private static String SAMPLE_XML="wydc-etl-sample.xml";
  //作业列表
  ArrayList<SampleNode> jobs=new ArrayList<SampleNode>();
  //转换列表
  ArrayList<SampleNode> trans=new ArrayList<SampleNode>();
  //返回的文件名
  String sampleFile;


  //tab页
  private CTabFolder wNoteFolder;
  private FormData fdNoteFolder;
  private CTabItem wNoteContentTab, wNoteFontTab;
  private FormData fdNoteContentComp, fdNoteFontComp;

  //job和trans选择
  private Label lbJobOrTrans;
  private CCombo ccbJobOrTrans;
  private FormData fdlbJobOrTrans, fdccbJobOrTrans;

  //样例列表
  private Label lbSample;
  private List lstSample;
  private FormData fdlbSample, fdlstSample;

  //描述
  private Label lbDesc;
  private StyledTextComp txtDesc;
  private FormData fdlbDesc, fdtxtDesc;

  private Button wOK, wCancel;
  private Listener lsOK, lsCancel;

  private Shell shell;
  private String title;
  private PropsUI props;

  private static GUIResource guiresource = GUIResource.getInstance();

  public static RGB COLOR_RGB_BLACK = guiresource.getColorBlack().getRGB();
  public static RGB COLOR_RGB_YELLOW = guiresource.getColorYellow().getRGB();
  public static RGB COLOR_RGB_GRAY = guiresource.getColorGray().getRGB();

  private Color fontColor;
  private Color bgColor;
  private Color borderColor;

  private Font font;

  private VariableSpace variables;

  /**
   * Dialog to allow someone to show or enter a text in variable width font
   *
   * @param parent
   *          The parent shell to use
   * @param title
   *          The dialog title
   */
  public SampleSelectDialog(Shell parent, String title) throws KettleException {
    super( parent, SWT.NONE );
    props = PropsUI.getInstance();
    this.title = title;
      LoadXml();
  }

  //读取xml文件加载作业和转换样例信息
  private void LoadXml() throws KettleException {
    InputStream is=SampleSelectDialog.class.getClassLoader().getResourceAsStream(SAMPLE_XML);
    try {
      InputStream inputStream = getClass().getResourceAsStream( SAMPLE_XML );
      if ( inputStream == null ) {
        inputStream = getClass().getResourceAsStream( "/" + SAMPLE_XML );
      }

      if ( inputStream == null ) {
        throw new KettleException( "Unable to find sample define file: "
                + SAMPLE_XML );
      }
      Document document = XMLHandler.loadXMLFile( inputStream, null, true, false );

      Node sampleNode = XMLHandler.getSubNode( document, "sample" );
      Node node=XMLHandler.getSubNode( sampleNode, "jobs" );
      java.util.List<Node> jobNodes = XMLHandler.getNodes( node, "job" );
      for ( Node subNode : jobNodes ) {
        LoadFromXmlResource( subNode,jobs );
      }
      node = XMLHandler.getSubNode( sampleNode, "trans" );
      java.util.List<Node> tranNodes = XMLHandler.getNodes( node, "tran" );
      for ( Node subNode : tranNodes ) {
        LoadFromXmlResource( subNode,trans );
      }

    } catch ( KettleXMLException e ) {
      throw new KettlePluginException( "Unable to read the kettle steps XML config file: " + SAMPLE_XML, e );
    }
  }

  //加载xml配置
  private void LoadFromXmlResource(Node node, ArrayList<SampleNode> jobs) throws KettleException, KettleXMLException {
    String name = getTagOrAttribute( node, "name" );
    String desc = getTagOrAttribute( node, "desc" );
    String file = getTagOrAttribute( node, "file" );
    SampleNode sample=new SampleNode();
    sample.name=name;
    sample.desc=desc;
    sample.fileName=file;
    jobs.add(sample);
  }

  protected String getTagOrAttribute( Node pluginNode, String tag ) {
    String string = XMLHandler.getTagValue( pluginNode, tag );
    if ( string == null ) {
      string = XMLHandler.getTagAttribute( pluginNode, tag );
    }
    return string;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.NONE );
    props.setLook( shell );
    shell.setImage( guiresource.getImageNoteSmall() );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( title );

    int margin = Const.MARGIN;
    int middle = 30;


    /*wNoteFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wNoteFolder, PropsUI.WIDGET_STYLE_TAB );

    wNoteContentTab = new CTabItem( wNoteFolder, SWT.NONE );
    //wNoteContentTab.setText( BaseMessages.getString( PKG, "NotePadDialog.ContentTab.Note" ) );
    Composite wNoteContentComp = new Composite( wNoteFolder, SWT.NONE );
    props.setLook( wNoteContentComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wNoteContentComp.setLayout( fileLayout );*/

    // 作业和转换选择
    lbJobOrTrans = new Label( shell,SWT.NONE );
    lbJobOrTrans.setText( BaseMessages.getString( PKG, "SampleSelectDialog.JobOrTransSelect.Label" ) );
    props.setLook( lbJobOrTrans );
    fdlbJobOrTrans = new FormData();
    fdlbJobOrTrans.left = new FormAttachment(margin, 0);
    fdlbJobOrTrans.top = new FormAttachment( 0, margin );
    fdlbJobOrTrans.right = new FormAttachment( middle, -8*margin);
    lbJobOrTrans.setLayoutData( fdlbJobOrTrans );
    ccbJobOrTrans = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    ccbJobOrTrans.setItems(new String[]{"作业","转换"});
    props.setLook( ccbJobOrTrans );
    fdccbJobOrTrans = new FormData();
    fdccbJobOrTrans.left = new FormAttachment( middle-margin, 0 );
    fdccbJobOrTrans.top = new FormAttachment( 0,margin );
    fdccbJobOrTrans.right = new FormAttachment( 100, -10);
    ccbJobOrTrans.setLayoutData( fdccbJobOrTrans );
    ccbJobOrTrans.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        refreshList();
      }
    } );
    ccbJobOrTrans.select( 0 );

    // 样例选择
    lbSample = new Label( shell,SWT.NONE );
    lbSample.setText( BaseMessages.getString( PKG, "SampleSelectDialog.SampleSelect.Label" ) );
    props.setLook( lbSample );
    fdlbSample = new FormData();
    fdlbSample.left = new FormAttachment( margin, 0 );
    fdlbSample.top = new FormAttachment( lbJobOrTrans, margin );
    fdlbSample.right = new FormAttachment( middle,0 );
    lbSample.setLayoutData( fdlbSample );
    lstSample = new List( shell, SWT.BORDER | SWT.READ_ONLY|SWT.V_SCROLL);
    //ccbJobOrTrans.setItems( Const.GetAvailableFontNames() );
    props.setLook( lstSample );
    fdlstSample = new FormData();
    fdlstSample.left = new FormAttachment(margin, 0 );
    fdlstSample.top = new FormAttachment( lbSample,  margin );
    fdlstSample.right = new FormAttachment( 100, -10 );
    fdlstSample.bottom = new FormAttachment( 100, -50*margin );
    lstSample.setLayoutData( fdlstSample );
    lstSample.select( 0 );
    lstSample.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        refreshSampleDesc();
      }
    } );


    //描述
    // From step line
    lbDesc = new Label( shell, SWT.NONE );
    lbDesc.setText( BaseMessages.getString( PKG, "SampleSelectDialog.SampleDesc.Label" ) );
    props.setLook( lbDesc );
    fdlbDesc = new FormData();
    fdlbDesc.left = new FormAttachment( margin, 0);
    fdlbDesc.top = new FormAttachment( lstSample, margin );
    fdlbDesc.right = new FormAttachment( middle, 0 );
    lbDesc.setLayoutData( fdlbDesc );
    txtDesc =
            new StyledTextComp( variables, shell, SWT.MULTI|SWT.WRAP
                    | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL, "" );

    txtDesc.setText( "" );
    txtDesc.setEnabled(false);
    txtDesc.setEditable(false);
    //props.setLook(txtDesc, PropsUI.WIDGET_STYLE_FIXED);
    props.setLook(txtDesc);
    fdtxtDesc = new FormData();
    fdtxtDesc.left = new FormAttachment(margin, 0 );
    fdtxtDesc.top = new FormAttachment( lbDesc, margin );
    fdtxtDesc.right = new FormAttachment( 100, -10 );
    fdtxtDesc.bottom = new FormAttachment( 100, -12*margin );
    txtDesc.setLayoutData( fdtxtDesc );

    /*fdNoteContentComp = new FormData();
    fdNoteContentComp.left = new FormAttachment( 0, 0 );
    fdNoteContentComp.top = new FormAttachment( 0, 0 );
    fdNoteContentComp.right = new FormAttachment( 100, 0 );
    fdNoteContentComp.bottom = new FormAttachment( 100, 0 );
    wNoteContentComp.setLayoutData( fdNoteContentComp );
    wNoteContentComp.layout();
    wNoteContentTab.setControl( wNoteContentComp );

    fdNoteFolder = new FormData();
    fdNoteFolder.left = new FormAttachment( 0, 0 );
    fdNoteFolder.top = new FormAttachment( 0, margin );
    fdNoteFolder.right = new FormAttachment( 100, 0 );
    fdNoteFolder.bottom = new FormAttachment( 100, -50 );
    wNoteFolder.setLayoutData( fdNoteFolder );*/
    // Some buttons

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, txtDesc );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    //BaseStepDialog.setSize( shell );
    shell.setSize(600, 600);
    Rectangle parentBounds = parent.getBounds();
    Rectangle shellBounds = shell.getBounds();

    shell.setLocation(parentBounds.x + (parentBounds.width - shellBounds.width)/2, parentBounds.y + (parentBounds.height - shellBounds.height)/2);

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return sampleFile;
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {
    ccbJobOrTrans.select(0);
    refreshList();
    refreshSampleDesc();
  }

  private void cancel() {
    sampleFile="";
    dispose();
  }

  private void ok() {
    String jobOrTrans=ccbJobOrTrans.getText();
    String name=lstSample.getSelection()[0].toString();
    SampleNode node=null;
    if("作业".equals(jobOrTrans)){
      node=getNode(name,jobs);
    }else{
      node=getNode(name,trans);
    }
    if(node!=null){
      sampleFile=node.fileName;
    }else{
      sampleFile="";
    }
    dispose();
  }

  //刷新列表
  private void refreshList() {
    String jobOrTrans=ccbJobOrTrans.getText();
    lstSample.removeAll();
    if("作业".equals(jobOrTrans)){
      fillListContent(lstSample,jobs);
    }else{
      fillListContent(lstSample,trans);
    }
    if(lstSample.getItemCount()>0){
      lstSample.select(0);
      refreshSampleDesc();
    }

  }

  //填充样例列表
  private void fillListContent(List lstSample,ArrayList<SampleNode> lst) {
    for(int i=0;i<lst.size();i++){
      lstSample.add(lst.get(i).name);
    }
  }

  //刷新样例描述
  private void refreshSampleDesc(){
    String jobOrTrans=ccbJobOrTrans.getText();
    String name=lstSample.getSelection()[0].toString();
    SampleNode node=null;
    if("作业".equals(jobOrTrans)){
      node=getNode(name,jobs);
    }else{
      node=getNode(name,trans);
    }
    if(node!=null){
      txtDesc.setText(node.desc);
    }else{
      txtDesc.setText("");
    }
  }

  //获取列表信息
  private SampleNode getNode(String name, ArrayList<SampleNode> lst) {
    SampleNode node=null;
    for(int i=0;i<lst.size();i++){
      if(name.equals(lst.get(i).name)){
        node=lst.get(i);
        break;
      }
    }
    return node;
  }

  public class SampleNode{
    //名称
    String name;
    //描述
    String desc;
    //文件名
    String fileName;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getDesc() {
      return desc;
    }

    public void setDesc(String desc) {
      this.desc = desc;
    }

    public String getFileName() {
      return fileName;
    }

    public void setFileName(String fileName) {
      this.fileName = fileName;
    }
  }
}
