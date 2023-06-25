# This is how easy it is to solve TikTok captchas

import cv2
import base64
import numpy as np

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import requests
from io import BytesIO

from scrape_utils import *


class PuzzleSolver:
    def __init__(self, puzzle, piece):
        self.puzzle = puzzle
        self.piece = piece

    def get_position(self):
        puzzle = self.__background_preprocessing()
        piece = self.__piece_preprocessing()
        matched = cv2.matchTemplate(
          puzzle, 
          piece, 
          cv2.TM_CCOEFF_NORMED
    )
        min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(matched)
        return max_loc[0]

    def __background_preprocessing(self):
        background = self.__sobel_operator(self.piece)
        return background

    def __piece_preprocessing(self):
        template = self.__sobel_operator(self.puzzle)
        return template

    def __sobel_operator(self, img):
        scale = 1
        delta = 0
        ddepth = cv2.CV_16S

        img = cv2.GaussianBlur(img, (3, 3), 0)
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        grad_x = cv2.Sobel(
            gray,
            ddepth,
            1,
            0,
            ksize=3,
            scale=scale,
            delta=delta,
            borderType=cv2.BORDER_DEFAULT,
        )
        grad_y = cv2.Sobel(
            gray,
            ddepth,
            0,
            1,
            ksize=3,
            scale=scale,
            delta=delta,
            borderType=cv2.BORDER_DEFAULT,
        )
        abs_grad_x = cv2.convertScaleAbs(grad_x)
        abs_grad_y = cv2.convertScaleAbs(grad_y)
        grad = cv2.addWeighted(abs_grad_x, 0.5, abs_grad_y, 0.5, 0)

        return grad

    def __img_to_grayscale(self, img):
        return cv2.imdecode(
          self.__string_to_image(img),
          cv2.IMREAD_COLOR
        )

    def __string_to_image(self, base64_string):

        return np.frombuffer(
          base64.b64decode(base64_string),
          dtype="uint8"
        )


def get_opt_ang(base_img, insert_img, mask, kernel_size=51):
    res = 1e10
    angel_min = 0
    borders = get_mask_borders(mask)
    dilated_borders = dilate_mask(borders, kernel_size=kernel_size)
    angels = list(range(1, 360))
    for angle in angels[::2]:
        rotated = ndimage.rotate(insert_img, angle, reshape=False)
        rotated = rotated[insert_img[:,:,3] > 1]
        combined = insert_image(base_img, rotated, mask)
        sobel_img = sobel(combined)
        sobel_sum = sobel_img[dilated_borders].sum()
        if sobel_sum < res:
            res = sobel_sum
            angel_min = angle

    return angel_min


def try_solve_capture_round(driver):
    while True:
        try:
            element = driver.find_element(by=By.ID,value='tiktok-verify-ele')
            drag_element = element.find_elements(By.CLASS_NAME,value='secsdk-captcha-drag-icon')[0]
            drag_line = element.find_elements(By.CLASS_NAME,value='captcha_verify_slide--slidebar')[0]
            line_width = drag_line.size['width'] - drag_element.size['width']
            imgs = element.find_elements(By.TAG_NAME,value='img')
        except BaseException as e:
            break

        base_img = get_image_by_url(
            imgs[0].get_property('src')
        )
        insert_img = get_image_by_url(
            imgs[1].get_property('src')
        )
        mask = get_center_mask(base_img)

        angel_min = get_opt_ang(base_img, insert_img, mask, kernel_size=15)

        offset_pixels = int(line_width * (360 - angel_min) / 360.0)

        swipe_elem(driver, drag_element, offset_pixels)

        driver.implicitly_wait(5)
        time.sleep(5)


def try_solve_capture_puzzle(driver):
    while True:
        try:
            element = driver.find_element(by=By.ID,value='tiktok-verify-ele')
            imgs = element.find_elements(By.TAG_NAME,value='img')
            drag_element = element.find_elements(By.CLASS_NAME,value='secsdk-captcha-drag-icon')[0]
        except BaseException as e:
            break

        base_img = get_image_by_url(imgs[0].get_property('src'))
        w,h = int(imgs[0].size['width']), int(imgs[0].size['height'])
        base_img = cv2.resize(base_img, (w,h))

        insert_img = get_image_by_url(
        imgs[1].get_property('src')
        )
        w,h = int(imgs[1].size['width']), int(imgs[1].size['height'])
        insert_img = cv2.resize(insert_img, (w,h))

        puzzle_solver = PuzzleSolver(base_img, insert_img)

        pos = puzzle_solver.get_position()
        swipe_elem(driver, drag_element, pos)

        driver.implicitly_wait(5)
        time.sleep(5)


phrase2solver = {
    'Drag the slider to fit the puzzle': try_solve_capture_round,
    'Drag the puzzle piece into place': try_solve_capture_puzzle
}


def try_solve_capture(driver):
    try:
        element = driver.find_element(by=By.ID,value='tiktok-verify-ele')
        drag_line = element.find_elements(By.CLASS_NAME,value='captcha_verify_slide--slidebar')[0]
    except BaseException as e:
        return
    text = drag_line.text
    solver = phrase2solver[text]
    print('solving...')
    solver(driver)
    print('solve!')