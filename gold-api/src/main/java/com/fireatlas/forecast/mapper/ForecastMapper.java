package com.fireatlas.forecast.mapper;

import com.fireatlas.forecast.dto.ForecastItem;
import com.fireatlas.forecast.dto.IssueItem;
import com.fireatlas.forecast.entity.CellForecast;
import com.fireatlas.forecast.entity.ForecastIssue;
import com.fireatlas.h3.policy.H3Index;
import org.springframework.stereotype.Component;

/** Converte a previsao por celula nos DTOs de resposta; os indices H3 saem como vieram, em hexadecimal. */
@Component
public class ForecastMapper {

    public ForecastItem toItem(CellForecast f) {
        return new ForecastItem(H3Index.toHex(f.h3()), f.h3Region() == null ? null : H3Index.toHex(f.h3Region()),
                f.probability7d() == null ? 0.0 : f.probability7d(), f.detections24h(), f.detections3d(),
                f.detections7d(), f.frpMean7d(), f.modisBrightnessMean7d(), f.viirsBrightnessMean7d(),
                f.ring1Detections24h(), f.ring1Detections7d(), f.ring2Detections7d(), f.active());
    }

    public IssueItem toItem(ForecastIssue i) {
        return new IssueItem(i.issueDate(), i.cells(), i.activeCells());
    }
}
