import matplotlib
import matplotlib.font_manager as fm

#  1663751271

axes_size = 2
legend_fontsize=12
legend_properties = {'weight':'bold','size':13}

fontsize_2 = 14
tickssize_2 = 12

fontsize_3 = 22
tickssize_3 = 20

subfontsize_3 = 17
subtickssize_3 = 15
def get_figsize(w, h, dpi=100):
        return [w * 0.3937008 * dpi / 100, h * 0.3937008 * dpi / 100]

def default_figsize2():
        return get_figsize(3, 2, dpi=300)

def small_figsize2():
        return get_figsize(3, 1.5, dpi=300)

def width_figsize():
        return get_figsize(6,1.5,dpi=300)

def adjust_label_font(a,label_font):
        label_font['fontsize'] = label_font['fontsize'] +a
        return label_font

xylabel_fontdict = {'fontsize': fontsize_2, 'fontweight': 'bold'}
xyticks_dict = {'fontsize': tickssize_2, 'fontweight': 'bold'}

xylabel_fontdict_3 = {'fontsize': fontsize_3, 'fontweight': 'bold'}

xyticks_dict_3 = {'fontsize': tickssize_3, 'fontweight': 'bold'}

xyticks_bold = { 'fontweight': 'bold'}

subxylabel_fontdict_3 = {'fontsize': subfontsize_3, 'fontweight': 'bold'}
subxyticks_dict_3 = {'fontsize': subtickssize_3, 'fontweight': 'bold'}

legend_dict = {'fontsize': legend_fontsize, 'columnspacing': 0.3, 'handletextpad':0.3}

# colors=['tab:blue','deepskyblue','tab:orange', 'orange', 'tab:green', 'limegreen', 'slategray', 'lightsteelblue', 'orangered', 'lightsalmon', 'palevioletred', 'pink']
colors = ['#D62728', 'tab:blue','tab:green','tab:orange', '#8B008B','slategray','pink']
bk_colors=['#E7E6E6','#D0CECE','#AFABAB','#767171']

marks = [ "o", "d", "s", "^", "X", "*",]
hatchs = ["////","\\","--","++","xx","..","oo","OO","**","--","++","xx","..","oo","OO","**"]

method2config = {
"Cuki": {"linestyle": "-", "marker":marks[0], "color": colors[0], 'label':'Cuki (ours)'},
"Cuki-OA": {"linestyle": "--", "marker":marks[1],  "color": colors[1], 'label':'Cuki-OA (ours)'},
"MBF": {"linestyle": "-.", "marker":marks[2],  "color": colors[2],'label':'MBF'},
"ClockSketch": {"linestyle": ":", "marker":marks[3],  "color": colors[3],'label':'ClockSketch'},
'SlidingSketch': {"linestyle": "-", "marker":marks[4], "color": colors[4],'label':'SlidingSketch'},
"SWAMP":{"linestyle": "--", "marker":marks[5],  "color": colors[5],'label':'SWAMP'},
}

method2configStability = {
"Cuki": {"linestyle": "-",  "color": colors[0], 'label':'Cuki (ours)'},
"Cuki-OA": {"linestyle": "--",  "color": colors[1], 'label':'Cuki-OA (ours)'},
"MBF": {"linestyle": "-.",   "color": colors[2],'label':'MBF'},
"ClockSketch": {"linestyle": ":",   "color": colors[3],'label':'ClockSketch'},
'SlidingSketch': {"linestyle": "-",  "color": colors[4],'label':'SlidingSketch'},
"SWAMP":{"linestyle": "--",   "color": colors[5],'label':'SWAMP'},
}

method2configRealWorld = {
"Cuki": {"linestyle": "-",  "color": colors[0], 'label':'Cuki'},
"MBF": {"linestyle": "-.",   "color": colors[2],'label':'MBF'},
"512mb": {"linestyle": ":",   "color": colors[3],'label':'1GB'},
"1gb": {"linestyle": ":",   "color": colors[4],'label':'2GB'},
"5gb": {"linestyle": ":",   "color": colors[1],'label':'10GB'},


"Real": {"linestyle": ":",   "color": colors[3],'label':'Real'},
"Alluxio": {"linestyle": ":",   "color": colors[4],'label':'Alluxio'},

"slave39":{"linestyle": "-.",   "color": colors[1],'label':'worker 1'},
"slave40":{"linestyle": "--",   "color": colors[2],'label':'worker 2'},
"total":{"linestyle": "-",   "color": colors[0],'label':'total'},
}

method2configMRC = {
        "Cuki": {"linestyle": "-", "marker":marks[0], "color": colors[0], 'label':'Cuki (ours)'},
        "Rar": {"linestyle": "-.", "marker":marks[2],  "color": colors[2],'label':'MBF'}
}


ticks_font = fm.FontProperties(family='Arial', style='normal', size='small', weight='bold', stretch='normal')
ticks_font_label = fm.FontProperties(family='SimHei', style='normal', size='medium', weight='bold', stretch='normal')