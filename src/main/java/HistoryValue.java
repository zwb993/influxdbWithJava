import lombok.Data;

import java.util.Date;

/**
 * @author zhengweibing3
 * @version V1.0.0
 * @description:
 * @date 2019/11/26 11:04
 * @copyright Copyright © 2019  智能城市icity.jd.com ALL Right Reserved
 */
@Data

public class HistoryValue {

    private Date time;
    private double value;
    private int unitNo;
}