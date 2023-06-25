import cv2
import numpy as np
from scipy import ndimage, optimize
import matplotlib.pyplot as plt
from tqdm import tqdm
from PIL import Image
import requests
from io import BytesIO
import time
import numpy as np

from selenium.webdriver.common.action_chains import ActionChains
from geopy.geocoders import Nominatim


p = plt.imshow


def get_location_by_city(city_name):
    geolocator = Nominatim(user_agent="geoapiExercises")
    location = geolocator.geocode(city_name)
    return location.latitude, location.longitude


def get_image_by_url(url):
    response = requests.get(url)
    img = Image.open(BytesIO(response.content))
    img = np.array(img)
    return img


def get_center_mask(img, alpha_threshold=50):
    """
    Create a mask for the region in the center of the image.

    Args:
        img (np.ndarray): Input image. Expected to have 4 channels (RGBA).
        alpha_threshold (int): Alpha value threshold to define the transparency.

    Returns:
        center_mask (np.ndarray): Boolean mask of the central region.
    """
    # Generate the mask
    mask = img[:,:,3] < alpha_threshold

    # Label each separate region in the mask
    labeled_mask, num_labels = ndimage.label(mask)

    # Calculate the centroid of each region
    centroids = ndimage.measurements.center_of_mass(mask, labels=labeled_mask, index=range(1, num_labels+1))

    # Convert to numpy array for convenience
    centroids = np.array(centroids)

    # Calculate the center of the image
    center = np.array([img.shape[0] / 2, img.shape[1] / 2])

    # Calculate the distance from each centroid to the center of the image
    distances = np.sqrt(np.sum((centroids - center) ** 2, axis=1))

    # Find the label of the region closest to the center of the image
    center_label = np.argmin(distances) + 1

    # Create a mask for the region closest to the center of the image
    center_mask = (labeled_mask == center_label)

    return center_mask


def sobel(img):
    img_gray = cv2.cvtColor(img,cv2.COLOR_RGBA2GRAY)
    sobelx = cv2.Sobel(img_gray,cv2.CV_64F,1,0,ksize=5)
    sobely = cv2.Sobel(img_gray,cv2.CV_64F,0,1,ksize=5)
    abs_grad_x = cv2.convertScaleAbs(sobelx)
    abs_grad_y = cv2.convertScaleAbs(sobely)
    
    grad = cv2.addWeighted(abs_grad_x, 0.5, abs_grad_y, 0.5, 0)
    return grad


def get_mask_borders(mask):
    mask = mask.astype(float)
    # Define a convolution kernel that sums up the 8 neighboring pixels around each pixel
    kernel = np.array([[1, 1, 1],
                       [1, 0, 1],
                       [1, 1, 1]])

    # Apply the convolution
    neighbors_sum = ndimage.convolve(mask, kernel, mode='constant', cval=0.0)

    # A border pixel will have a value of 1 (because it's part of the mask)
    # and its neighbors_sum will be less than 8 (because it has at least one neighbor not part of the mask)
    borders = np.logical_and(mask == 1, neighbors_sum < 8)

    return borders > 0

def dilate_mask(mask, kernel_size=7, iterations=1):
    # Create a kernel
    mask = mask.astype(np.uint8)
    kernel = np.ones((kernel_size, kernel_size), np.uint8)

    # Dilate the mask
    dilated_mask = cv2.dilate(mask, kernel, iterations)

    return dilated_mask > 0


def zero_last_ones(mask, num):
    count = 0
    for idx in np.argwhere(mask)[::-1]:
        if count >= num:
            break
        if mask[tuple(idx)] == 1:
            mask[tuple(idx)] = 0
            count += 1
    return mask


def insert_image(base_img, insert_img, mask):
    base_img_cp = base_img.copy()
    mask_sum = mask.sum()
    min_val = min(mask_sum, len(insert_img))
    mask = zero_last_ones(mask, mask_sum - min_val)
    base_img_cp[mask] = insert_img[:min_val]
    return base_img_cp


def swipe_elem(driver, drag_element, offset_pixels):
    action = ActionChains(driver)
    action.click_and_hold(drag_element)
    action.move_by_offset(offset_pixels, 0)
    action.release().perform()

def tiktok_username2link(tiktok_username):
    return f'https://www.tiktok.com/@{tiktok_username}'
