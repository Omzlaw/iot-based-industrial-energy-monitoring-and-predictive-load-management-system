<mxfile host="app.diagrams.net">
  <diagram name="IoT Energy Monitoring & Hybrid Control">
    <mxGraphModel dx="1200" dy="800" grid="1" gridSize="10" guides="1" tooltips="1" connect="1">
      <root>
        <mxCell id="0"/>
        <mxCell id="1" parent="0"/>

        <!-- Physical Process -->
        <mxCell id="process" value="Physical Process&#xa;(Motor / Pump / Load / Tank)" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#dae8fc;strokeColor=#6c8ebf;" vertex="1" parent="1">
          <mxGeometry x="40" y="200" width="240" height="80" as="geometry"/>
        </mxCell>

        <!-- Sensors -->
        <mxCell id="sensors" value="Sensors&#xa;- Current Sensor (ACS712)&#xa;- Voltage Sensor&#xa;- Ultrasonic / Env Sensors" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#fff2cc;strokeColor=#d6b656;" vertex="1" parent="1">
          <mxGeometry x="330" y="200" width="260" height="100" as="geometry"/>
        </mxCell>

        <!-- ESP32 -->
        <mxCell id="esp32" value="ESP32 (Edge Controller)&#xa;- Power & Energy Calculation&#xa;- Rule-Based Control&#xa;- MQTT Client" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#d5e8d4;strokeColor=#82b366;" vertex="1" parent="1">
          <mxGeometry x="660" y="180" width="300" height="120" as="geometry"/>
        </mxCell>

        <!-- MQTT -->
        <mxCell id="mqtt" value="MQTT Broker&#xa;(Cloud / Local)" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#f8cecc;strokeColor=#b85450;" vertex="1" parent="1">
          <mxGeometry x="1030" y="210" width="220" height="70" as="geometry"/>
        </mxCell>

        <!-- Python -->
        <mxCell id="python" value="Python Analytics & Prediction&#xa;- Data Logging&#xa;- Model Training (LSTM / Trend)&#xa;- Demand Prediction" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#e1d5e7;strokeColor=#9673a6;" vertex="1" parent="1">
          <mxGeometry x="1030" y="90" width="300" height="120" as="geometry"/>
        </mxCell>

        <!-- Control Back -->
        <mxCell id="control" value="Prediction & Risk Signals&#xa;(Peak Risk / Suggested Action)" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#f5f5f5;strokeColor=#666666;" vertex="1" parent="1">
          <mxGeometry x="1030" y="350" width="300" height="90" as="geometry"/>
        </mxCell>

        <!-- Edges -->
        <mxCell id="e1" style="endArrow=block;" edge="1" parent="1" source="process" target="sensors">
          <mxGeometry relative="1" as="geometry"/>
        </mxCell>

        <mxCell id="e2" style="endArrow=block;" edge="1" parent="1" source="sensors" target="esp32">
          <mxGeometry relative="1" as="geometry"/>
        </mxCell>

        <mxCell id="e3" style="endArrow=block;" edge="1" parent="1" source="esp32" target="mqtt">
          <mxGeometry relative="1" as="geometry"/>
        </mxCell>

        <mxCell id="e4" style="endArrow=block;" edge="1" parent="1" source="mqtt" target="python">
          <mxGeometry relative="1" as="geometry"/>
        </mxCell>

        <mxCell id="e5" style="endArrow=block;" edge="1" parent="1" source="python" target="mqtt">
          <mxGeometry relative="1" as="geometry"/>
        </mxCell>

        <mxCell id="e6" style="endArrow=block;" edge="1" parent="1" source="mqtt" target="esp32">
          <mxGeometry relative="1" as="geometry"/>
        </mxCell>

        <mxCell id="e7" style="endArrow=block;" edge="1" parent="1" source="esp32" target="process">
          <mxGeometry relative="1" as="geometry"/>
        </mxCell>

      </root>
    </mxGraphModel>
  </diagram>
</mxfile>
