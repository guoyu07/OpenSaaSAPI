


apple_jx = {
    "iPhone1_1": "iPhone",
    "iPhone1_2": "iPhone",
    "iPhone2_1": "iPhone 3GS",

    "iPhone3_1": "iPhone 4",
    "iPhone3_2": "iPhone 4",
    "iPhone3_3": "iPhone 4",

    "iPhone4_1": "iPhone 4S",

    "iPhone5_1": "iPhone 5",
    "iPhone5_2": "iPhone 5",

    "iPhone5_3": "iPhone 5c",
    "iPhone5_4": "iPhone 5c",

    "iPhone6_1": "iPhone 5s",
    "iPhone6_2": "iPhone 5s",

    "iPhone7_2": "iPhone 6",

    "iPhone7_1": "iPhone 6 Plus",

    "iPhone8_1": "iPhone 6s",

    "iPhone8_2": "iPhone 6s Plus",

    "iPhone8_4": "iPhone SE",

    "iPhone9_1": "iPhone 7",
    "iPhone9_3": "iPhone 7",
    "iPhone9_2": "iPhone 7 Plus",
    "iPhone9_4": "iPhone 7 Plus",

    "iPad2_5": "iPad mini",
    "iPad2_6": "iPad mini",
    "iPad2_7": "iPad mini",
    "iPad4_4": "iPad mini 2",
    "iPad4_5": "iPad mini 2",
    "iPad4_6": "iPad mini 2",
    "iPad4_7": "iPad mini 3",
    "iPad4_8": "iPad mini 3",
    "iPad4_9": "iPad mini 3",
    "iPad5_1": "iPad mini 4",
    "iPad5_2": "iPad mini 4",

    "iPad1_1": "iPad",
    "iPad2_1": "iPad 2",
    "iPad2_2": "iPad 2",
    "iPad2_3": "iPad 2",
    "iPad2_4": "iPad 2",
    "iPad3_1": "iPad 3",
    "iPad3_2": "iPad 3",
    "iPad3_3": "iPad 3",
    "iPad3_4": "iPad 4",
    "iPad3_5": "iPad 4",
    "iPad3_6": "iPad 4",
    "iPad5_3": "iPad Air 2",
    "iPad5_4": "iPad Air 2",
    "iPad6_7": "iPad Pro (12.9 inch)",
    "iPad6_8": "iPad Pro (12.9 inch)",
    "iPad6_3": "iPad Pro (9.7 inch)",
    "iPad6_4": "iPad Pro (9.7 inch)",
    "iPad4_1": "iPad Air",
    "iPad4_2": "iPad Air",
    "iPad4_3": "iPad Air"
}


[apple_jx.setdefault(key.lower(), apple_jx[key]) for key in apple_jx.keys()]