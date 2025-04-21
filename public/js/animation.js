function fadeIn(element, duration = 400) {
  element.style.opacity = 0;
  element.style.display = 'block';

  let start = null;

  function animation(timestamp) {
    if (!start) start = timestamp;
    const progress = timestamp - start;
    element.style.opacity = Math.min(progress / duration, 1);

    if (progress < duration) {
      requestAnimationFrame(animation);
    }
  }

  requestAnimationFrame(animation);
}

function fadeOut(element, duration = 400) {
  element.style.opacity = 1;

  let start = null;

  function animation(timestamp) {
    if (!start) start = timestamp;
    const progress = timestamp - start;
    element.style.opacity = Math.max(1 - progress / duration, 0);

    if (progress < duration) {
      requestAnimationFrame(animation);
    } else {
      element.style.display = 'none';
    }
  }

  requestAnimationFrame(animation);
}

export { fadeIn, fadeOut };