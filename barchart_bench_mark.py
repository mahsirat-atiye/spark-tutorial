import json
import pandas as pd
import plotly.express as px

slack_msg = """• ChurnLifetimeOneTimer        | preview:   53.60 | compute:  0.83 | persist:  8.38 | total:   62.82
• DaysSinceLastCheckout        | preview:   22.65 | compute:  0.40 | persist:  3.12 | total:   26.16
• DaysSinceLastDeferredPayment | preview:   23.15 | compute:  0.57 | persist:  3.29 | total:   27.03
• DaysSinceLastInteraction     | preview:   20.18 | compute:  0.34 | persist:  3.11 | total:   23.63
• DaysSinceLastPush            | preview:   19.10 | compute:  0.46 | persist:  3.03 | total:   22.60
• FirstLastCheckoutTime        | preview:  435.95 | compute:  0.43 | persist:  3.80 | total:  440.18
• FirstLastDeferredPaymentTime | preview:  130.88 | compute:  0.52 | persist:  3.63 | total:  135.03
• FirstLastInteractions        | preview:   19.14 | compute:  0.41 | persist:  2.85 | total:   22.40
• FirstLastPushTime            | preview:  102.27 | compute:  0.20 | persist:  2.80 | total:  105.27
• MetricsAgg                   | preview:   20.39 | compute:  0.31 | persist:  3.07 | total:   23.77
• NewUserLastN                 | preview:   19.49 | compute:  0.40 | persist:  3.07 | total:   22.96
• PageTimeAgg                  | preview:   19.08 | compute:  0.32 | persist:  2.90 | total:   22.29
• PeakHourMode                 | preview:  139.74 | compute:  0.30 | persist:  2.96 | total:  143.00
• PurchaseOneTimer             | preview:   19.34 | compute:  0.32 | persist:  2.78 | total:   22.44
• PurchaseReturned             | preview:   34.35 | compute:  0.32 | persist:  2.65 | total:   37.33
• Returned                     | preview:   29.79 | compute:  0.29 | persist:  3.00 | total:   33.08
• UserIsActiveLastN            | preview:   19.28 | compute:  0.31 | persist:  2.82 | total:   22.40"""

import json
import pandas as pd
import plotly.express as px
def draw_plot(slack_msg):
    slack_msg = slack_msg.replace("• ", "}, { 'task' : '")
    slack_msg = slack_msg[3:]
    slack_msg = slack_msg.replace("| preview:", "', 'preview': ")
    slack_msg = slack_msg.replace("| compute:", ", 'compute': ")
    slack_msg = slack_msg.replace("| persist:", " , 'persist': ")
    slack_msg = slack_msg.replace("| total:", ", 'total': ")
    slack_msg = slack_msg.replace("\n", " ")
    slack_msg = "{'results': [" + slack_msg + "}]}"
    slack_msg = slack_msg.replace("'", "\"")
    json1_data = json.loads(slack_msg)
    b = dict()
    i = 0
    for elem in json1_data['results']:
        b[i] = {"task": elem['task'], "part": "preview", "time": elem['preview']}
        i += 1
        b[i] = {"task": elem['task'], "part": "persist", "time": elem['persist']}
        i += 1
        b[i] = {"task": elem['task'], "part": "compute", "time": elem['compute']}
        i += 1

    df = pd.DataFrame(b)
    df = df.transpose()
    fig = px.bar(df, x="task", y="time", color="part", title="Compare time (Preview, Compute, Persist)")
    fig.show()


if __name__ == '__main__':
    draw_plot(slack_msg)
